import time
import threading
from typing import Optional


class SnowflakeIDGenerator:
    """
    雪花算法单机版ID生成器
    
    雪花算法生成的ID结构（64位）：
    - 1位符号位（固定为0）
    - 41位时间戳（毫秒级，可使用约69年）
    - 10位机器ID（单机版固定为0）
    - 12位序列号（同一毫秒内的序列号，最大4095）
    
    特点：
    - 趋势递增：大致按照时间递增
    - 不重复：在单机环境下保证不重复
    - 高性能：内存生成，无需访问数据库
    - 可排序：生成的ID可以按时间排序
    """
    
    def __init__(self, machine_id: int = 0, epoch: int = 1640995200000):
        """
        初始化雪花算法ID生成器
        
        参数:
            machine_id (int): 机器ID，单机版可以固定为0，范围0-1023
            epoch (int): 起始时间戳（毫秒），默认为2022-01-01 00:00:00 UTC
        """
        # 各部分位数
        self.TIMESTAMP_BITS = 41
        self.MACHINE_ID_BITS = 10
        self.SEQUENCE_BITS = 12
        
        # 最大值
        self.MAX_MACHINE_ID = (1 << self.MACHINE_ID_BITS) - 1  # 1023
        self.MAX_SEQUENCE = (1 << self.SEQUENCE_BITS) - 1      # 4095
        
        # 位移量
        self.MACHINE_ID_SHIFT = self.SEQUENCE_BITS              # 12
        self.TIMESTAMP_SHIFT = self.SEQUENCE_BITS + self.MACHINE_ID_BITS  # 22
        
        # 验证机器ID
        if machine_id < 0 or machine_id > self.MAX_MACHINE_ID:
            raise ValueError(f"机器ID必须在0-{self.MAX_MACHINE_ID}之间")
        
        self.machine_id = machine_id
        self.epoch = epoch
        self.sequence = 0
        self.last_timestamp = -1
        
        # 线程锁，确保线程安全
        self._lock = threading.Lock()
    
    def _current_timestamp(self) -> int:
        """获取当前时间戳（毫秒）"""
        return int(time.time() * 1000)
    
    def _wait_next_millis(self, last_timestamp: int) -> int:
        """等待下一毫秒"""
        timestamp = self._current_timestamp()
        while timestamp <= last_timestamp:
            timestamp = self._current_timestamp()
        return timestamp
    
    def generate_id(self) -> int:
        """
        生成雪花算法ID
        
        返回:
            int: 64位整数ID
        
        异常:
            RuntimeError: 当时钟回拨时抛出异常
        """
        with self._lock:
            timestamp = self._current_timestamp()
            
            # 检查时钟回拨
            if timestamp < self.last_timestamp:
                raise RuntimeError(
                    f"时钟回拨检测：当前时间戳{timestamp}小于上次时间戳{self.last_timestamp}"
                )
            
            # 同一毫秒内
            if timestamp == self.last_timestamp:
                self.sequence = (self.sequence + 1) & self.MAX_SEQUENCE
                # 序列号溢出，等待下一毫秒
                if self.sequence == 0:
                    timestamp = self._wait_next_millis(self.last_timestamp)
            else:
                # 新的毫秒，序列号重置为0
                self.sequence = 0
            
            self.last_timestamp = timestamp
            
            # 组装ID
            snowflake_id = (
                ((timestamp - self.epoch) << self.TIMESTAMP_SHIFT) |
                (self.machine_id << self.MACHINE_ID_SHIFT) |
                self.sequence
            )
            
            return snowflake_id
    
    def generate_string_id(self) -> str:
        """
        生成字符串格式的雪花算法ID
        
        返回:
            str: 字符串格式的ID
        """
        return str(self.generate_id())
    
    def parse_id(self, snowflake_id: int) -> dict:
        """
        解析雪花算法ID，提取各部分信息
        
        参数:
            snowflake_id (int): 雪花算法生成的ID
        
        返回:
            dict: 包含时间戳、机器ID、序列号的字典
        """
        # 提取各部分
        sequence = snowflake_id & self.MAX_SEQUENCE
        machine_id = (snowflake_id >> self.MACHINE_ID_SHIFT) & self.MAX_MACHINE_ID
        timestamp = (snowflake_id >> self.TIMESTAMP_SHIFT) + self.epoch
        
        return {
            "timestamp": timestamp,
            "datetime": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp / 1000)),
            "machine_id": machine_id,
            "sequence": sequence,
            "original_id": snowflake_id
        }


# 全局单例实例
_snowflake_generator: Optional[SnowflakeIDGenerator] = None
_generator_lock = threading.Lock()


def get_snowflake_generator(machine_id: int = 0) -> SnowflakeIDGenerator:
    """
    获取雪花算法ID生成器单例实例
    
    参数:
        machine_id (int): 机器ID，默认为0
    
    返回:
        SnowflakeIDGenerator: 雪花算法ID生成器实例
    """
    global _snowflake_generator
    
    if _snowflake_generator is None:
        with _generator_lock:
            if _snowflake_generator is None:
                _snowflake_generator = SnowflakeIDGenerator(machine_id=machine_id)
    
    return _snowflake_generator


def generate_snowflake_id() -> int:
    """
    生成雪花算法ID（便捷函数）
    
    返回:
        int: 64位整数ID
    """
    generator = get_snowflake_generator()
    return generator.generate_id()


def generate_snowflake_string_id() -> str:
    """
    生成字符串格式的雪花算法ID（便捷函数）
    
    返回:
        str: 字符串格式的ID
    """
    generator = get_snowflake_generator()
    return generator.generate_string_id()


def parse_snowflake_id(snowflake_id: int) -> dict:
    """
    解析雪花算法ID（便捷函数）
    
    参数:
        snowflake_id (int): 雪花算法生成的ID
    
    返回:
        dict: 包含时间戳、机器ID、序列号的字典
    """
    generator = get_snowflake_generator()
    return generator.parse_id(snowflake_id)


if __name__ == "__main__":
    # 测试代码
    print("雪花算法ID生成器测试")
    print("=" * 50)
    
    # 生成几个ID进行测试
    for i in range(5):
        snowflake_id = generate_snowflake_id()
        string_id = generate_snowflake_string_id()
        parsed = parse_snowflake_id(snowflake_id)
        
        print(f"ID {i+1}:")
        print(f"  整数ID: {snowflake_id}")
        print(f"  字符串ID: {string_id}")
        print(f"  解析结果: {parsed}")
        print()
        
        # 短暂延迟
        time.sleep(0.001)