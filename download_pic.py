import os
import threading
import time
import concurrent.futures
import requests
import pandas as pd

from concurrent.futures import ThreadPoolExecutor
from retrying import retry


class UrlDownloader:
    """
    URL下载器类，用于从给定的URL列表中下载文件。
    """
    ERROR_FILE_PATH = './url_download_err.txt'
    DUPLICATE_FILE_PATH = './url_download_dup.txt'
    MAX_WORKERS = 10

    def __init__(self, max_workers: int = MAX_WORKERS):
        """
        初始化URL下载器，设置错误和成功日志文件路径。

        参数:
            max_workers: 最大线程数，默认为10
        """
        self.write_lock = threading.Lock()
        self.url_lock = threading.Lock()
        self.path_lock = threading.Lock()
        self.download_locks = {}
        self.thread_pool_executor = ThreadPoolExecutor(max_workers=max_workers)
        self.error_file_path = UrlDownloader.ERROR_FILE_PATH
        self.duplicate_file_path = UrlDownloader.DUPLICATE_FILE_PATH
        self.max_workers = max_workers
        self.processed_urls = set()
        self.processed_paths = set()

    def write_to_file(self, data: str, file_path: str) -> None:
        """将数据写入指定文件。"""
        with self.write_lock:
            try:
                with open(file_path, 'a') as file:
                    file.write(data + '\n')
            except Exception as e:
                print(f"写入文件时发生异常：{e}")

    @retry(stop_max_attempt_number=5, wait_fixed=3000)
    def download_file(self, url: str, save_path: str) -> None:
        """下载指定URL的文件到指定路径。"""
        start_time = time.time()

        lock = self.download_locks.setdefault(url, threading.Lock())

        with lock:
            try:
                # 再次检查URL和文件路径是否已经被处理过
                with self.url_lock:
                    try:
                        if url in self.processed_urls:
                            print(f"URL已被处理过：{url}")
                            self.write_to_file(f"{url},{save_path},重复URL", self.duplicate_file_path)
                            return
                    finally:
                        pass

                with self.path_lock:
                    try:
                        if save_path in self.processed_paths:
                            print(f"文件路径已存在：{save_path}")
                            self.write_to_file(f"{url},{save_path},重复路径", self.duplicate_file_path)
                            return
                    finally:
                        pass

                # 创建保存路径的目录
                os.makedirs(os.path.dirname(save_path), exist_ok=True)

                session = requests.Session()
                response = session.get(url, timeout=30)
                if response.status_code == 200:
                    with open(save_path, 'wb') as f:
                        f.write(response.content)
                        end_time = time.time()
                    print(f"下载成功: {save_path},用时:{end_time - start_time:.2f}秒")
                    with self.url_lock:
                        self.processed_urls.add(url)
                    with self.path_lock:
                        self.processed_paths.add(save_path)
                else:
                    self.write_to_file(
                        f"'{url}': '{save_path}'",
                        self.error_file_path
                    )
                    print(f"下载失败: {url}, 状态码: {response.status_code}")
            except requests.exceptions.RequestException as e:
                raise e

    def batch_download(self, url_mapping: dict) -> None:
        """批量下载文件。"""
        futures = [
            self.thread_pool_executor.submit(self.download_file, url, path)
            for url, path in url_mapping.items()
        ]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"下载时发生异常：{e}")

    def process_row(self, row: pd.Series, chunk_list: dict) -> None:
        """处理CSV文件的每一行数据，提取URL和保存路径。"""
        data = row[0].split('\': \'')
        key = data[0].strip("'")
        value = data[1].strip("'")

        # 检查URL是否已经被处理过
        with self.url_lock:
            try:
                if key in self.processed_urls:
                    print(f"URL已被处理过：{key}")
                    self.write_to_file(f"{key},{value},重复URL", self.duplicate_file_path)
                    return
            finally:
                pass

        # 检查文件路径是否已经被处理过
        with self.path_lock:
            try:
                if value in self.processed_paths:
                    print(f"文件路径已存在：{value}")
                    self.write_to_file(f"{key},{value},重复文件路径", self.duplicate_file_path)
                    return
            finally:
                pass

        chunk_list[key] = value

    def read_and_process_csv(self, csv_file_path: str, chunk_size: int = 1) -> None:
        """读取CSV文件并批量下载文件。"""
        reader = pd.read_csv(
            csv_file_path, header=None, chunksize=chunk_size,
            encoding='utf-8', delimiter='@#$', engine='python'
        )
        for chunk in reader:
            chunk_data = {}
            chunk.apply(self.process_row, axis=1, args=(chunk_data,))
            self.batch_download(chunk_data)