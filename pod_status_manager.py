"""
Pod Status Manager using Redis (Optimized)

此模組提供基於 Redis 的 Pod 狀態管理功能，
採用單一 Hash 結構，實現高效能的狀態查詢與更新。

資料結構：
- Key: mlgame:pods:status (Hash)
- Field: pod_name
- Value: JSON {"status": "idle", "updated_at": 1234567890}
"""

import json
import time
from typing import Optional, Dict, List
from enum import Enum
from loguru import logger

try:
    import redis
except ImportError:
    logger.error("redis package not installed. Please run: pip install redis")
    raise


class PodStatus(str, Enum):
    """Pod 狀態列舉"""
    STARTING = "starting"   # 啟動中
    IDLE = "idle"           # 空閒中
    BUSY = "busy"           # 處理任務中
    ERROR = "error"         # 發生錯誤
    # 注意：完成後直接刪除，不需要 COMPLETED 狀態


class PodStatusManipulator:
    """
    Pod 狀態操作器（單一 Pod 視角）
    
    職責：
    - 註冊/註銷單一 pod
    - 更新單一 pod 狀態
    - 心跳保活
    """
    
    def __init__(
        self,
        redis_client: redis.Redis,
        pod_name: str,
        key_prefix: str = "pod-status"
    ):
        """
        初始化 Pod 狀態操作器
        
        Args:
            redis_client: Redis 客戶端實例
            pod_name: Pod 名稱
            key_prefix: Redis key 前綴
        """
        self.redis = redis_client
        self.pod_name = pod_name
        self.key_prefix = key_prefix
    
    def _create_status_data(self, status: PodStatus) -> str:
        """建立狀態資料（JSON 字串）"""
        return json.dumps({
            "status": status,
            "updated_at": int(time.time())
        })
    
    def register(self, initial_status: PodStatus = PodStatus.STARTING) -> bool:
        """
        註冊 pod（啟動時呼叫）
        
        Args:
            initial_status: 初始狀態（預設為 STARTING）
            
        Returns:
            是否成功
        """
        try:
            self.redis.hset(
                self.key_prefix,
                self.pod_name,
                self._create_status_data(initial_status)
            )
            logger.info(f"✅ Pod registered: {self.pod_name} ({initial_status})")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to register pod {self.pod_name}: {e}")
            return False
    
    def unregister(self) -> bool:
        """
        註銷 pod（關閉或完成時呼叫）
        
        Returns:
            是否成功
        """
        try:
            self.redis.hdel(self.key_prefix, self.pod_name)
            logger.info(f"✅ Pod unregistered: {self.pod_name}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to unregister pod {self.pod_name}: {e}")
            return False
    
    def set_status(self, status: PodStatus) -> bool:
        """
        設定 pod 狀態
        
        Args:
            status: 新狀態
            
        Returns:
            是否成功
        """
        try:
            self.redis.hset(
                self.key_prefix,
                self.pod_name,
                self._create_status_data(status)
            )
            logger.info(f"✅ Pod {self.pod_name} → {status}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to set status for {self.pod_name}: {e}")
            return False
    
    def set_starting(self) -> bool:
        """設定為啟動中"""
        return self.set_status(PodStatus.STARTING)
    
    def set_idle(self) -> bool:
        """設定為空閒"""
        return self.set_status(PodStatus.IDLE)
    
    def set_busy(self) -> bool:
        """設定為忙碌"""
        return self.set_status(PodStatus.BUSY)
    
    def set_error(self) -> bool:
        """設定為錯誤"""
        return self.set_status(PodStatus.ERROR)
    
    def heartbeat(self) -> bool:
        """
        更新心跳（只更新時間戳，保持狀態不變）
        
        Returns:
            是否成功
        """
        try:
            # 先取得目前狀態
            current_data = self.redis.hget(self.key_prefix, self.pod_name)
            if not current_data:
                logger.warning(f"⚠️ Pod {self.pod_name} not found, registering...")
                return self.register(PodStatus.IDLE)
            
            # 更新時間戳
            status_dict = json.loads(current_data)
            status_dict["updated_at"] = int(time.time())
            
            self.redis.hset(
                self.key_prefix,
                self.pod_name,
                json.dumps(status_dict)
            )
            return True
        except Exception as e:
            logger.error(f"❌ Failed to update heartbeat for {self.pod_name}: {e}")
            return False
    
    def get_status(self) -> Optional[Dict]:
        """
        取得當前 pod 的狀態
        
        Returns:
            狀態字典 {"status": "idle", "updated_at": 1234567890} 或 None
        """
        try:
            data = self.redis.hget(self.key_prefix, self.pod_name)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"❌ Failed to get status for {self.pod_name}: {e}")
            return None
    
    def __enter__(self):
        """Context manager 進入"""
        self.register()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager 退出"""
        self.unregister()


class PodStatusMonitor:
    """
    Pod 狀態監控器（全域視角）
    
    職責：
    - 查詢所有 pod 狀態（單次查詢）
    - 統計各狀態數量
    - 清理過期 pod
    """
    
    def __init__(
        self,
        redis_client: redis.Redis,
        key_prefix: str = "pod-status"
    ):
        """
        初始化 Pod 狀態監控器
        
        Args:
            redis_client: Redis 客戶端實例
            key_prefix: Redis key 前綴
        """
        self.redis = redis_client
        self.key_prefix = key_prefix
    
    def get_all_pods(self) -> Dict[str, Dict]:
        """
        取得所有 pod 及其狀態（只需一次 Redis 查詢！）
        
        Returns:
            {pod_name: {"status": "idle", "updated_at": 1234567890}}
        """
        try:
            # 一次 HGETALL 取得所有資料
            raw_data = self.redis.hgetall(self.key_prefix)
            
            # 解析 JSON
            pods = {}
            for pod_name, json_data in raw_data.items():
                try:
                    pods[pod_name] = json.loads(json_data)
                except json.JSONDecodeError as e:
                    logger.error(f"❌ Failed to parse data for {pod_name}: {e}")
            
            return pods
        except Exception as e:
            logger.error(f"❌ Failed to get all pods: {e}")
            return {}
    
    def get_status_summary(self) -> Dict[str, int]:
        """
        取得狀態摘要統計
        
        Returns:
            {
                "total": 10,
                "starting": 1,
                "idle": 5,
                "busy": 3,
                "error": 1
            }
        """
        pods = self.get_all_pods()
        
        summary = {
            "total": len(pods),
            PodStatus.STARTING: 0,
            PodStatus.IDLE: 0,
            PodStatus.BUSY: 0,
            PodStatus.ERROR: 0
        }
        
        for pod_data in pods.values():
            status = pod_data.get("status")
            if status in summary:
                summary[status] += 1
        
        return summary
    
    def get_pods_by_status(self, target_status: PodStatus) -> List[str]:
        """
        取得特定狀態的 pod 列表
        
        Args:
            target_status: 目標狀態
            
        Returns:
            Pod 名稱列表
        """
        pods = self.get_all_pods()
        return [
            pod_name 
            for pod_name, data in pods.items() 
            if data.get("status") == target_status
        ]
    
    def get_idle_pods(self) -> List[str]:
        """取得所有空閒的 pod"""
        return self.get_pods_by_status(PodStatus.IDLE)
    
    def get_busy_pods(self) -> List[str]:
        """取得所有忙碌的 pod"""
        return self.get_pods_by_status(PodStatus.BUSY)
    
    def get_error_pods(self) -> List[str]:
        """取得所有錯誤狀態的 pod"""
        return self.get_pods_by_status(PodStatus.ERROR)
    
    def get_starting_pods(self) -> List[str]:
        """取得所有啟動中的 pod"""
        return self.get_pods_by_status(PodStatus.STARTING)
    
    def cleanup_stale_pods(self, timeout_seconds: int = 600) -> int:
        """
        清理過期的 pod（超過指定時間未更新）
        
        Args:
            timeout_seconds: 超時時間（秒），預設 10 分鐘
            
        Returns:
            清理的 pod 數量
        """
        current_time = int(time.time())
        pods = self.get_all_pods()
        
        stale_pods = []
        for pod_name, data in pods.items():
            try:
                updated_at = data.get("updated_at", 0)
                if current_time - updated_at > timeout_seconds:
                    stale_pods.append(pod_name)
            except (ValueError, TypeError) as e:
                logger.error(f"❌ Invalid updated_at for {pod_name}: {e}")
                stale_pods.append(pod_name)  # 清理異常資料
        
        if not stale_pods:
            return 0
        
        try:
            # 批次刪除
            removed = self.redis.hdel(self.key_prefix, *stale_pods)
            logger.info(f"🧹 Cleaned {removed} stale pods: {stale_pods}")
            return removed
        except Exception as e:
            logger.error(f"❌ Failed to cleanup stale pods: {e}")
            return 0
    
    def get_pods_with_details(self) -> List[Dict]:
        """
        取得 pod 列表及詳細資訊（適合顯示）
        
        Returns:
            [
                {
                    "pod_name": "game-123",
                    "status": "idle",
                    "updated_at": 1234567890,
                    "uptime_seconds": 300
                },
                ...
            ]
        """
        pods = self.get_all_pods()
        current_time = int(time.time())
        
        details = []
        for pod_name, data in pods.items():
            detail = {
                "pod_name": pod_name,
                "status": data.get("status"),
                "updated_at": data.get("updated_at"),
                "inactive_seconds": current_time - data.get("updated_at", current_time)
            }
            details.append(detail)
        
        # 按更新時間排序（最新的在前）
        details.sort(key=lambda x: x["updated_at"], reverse=True)
        return details
    
    def remove_pod(self, pod_name: str) -> bool:
        """
        移除特定 pod（管理用途）
        
        Args:
            pod_name: Pod 名稱
            
        Returns:
            是否成功
        """
        try:
            removed = self.redis.hdel(self.key_prefix, pod_name)
            if removed:
                logger.info(f"✅ Pod removed: {pod_name}")
                return True
            else:
                logger.warning(f"⚠️ Pod not found: {pod_name}")
                return False
        except Exception as e:
            logger.error(f"❌ Failed to remove pod {pod_name}: {e}")
            return False
    
    def clear_all(self) -> bool:
        """
        清空所有 pod 狀態（危險操作，僅用於測試或維護）
        
        Returns:
            是否成功
        """
        try:
            self.redis.delete(self.key_prefix)
            logger.warning("⚠️ All pod statuses cleared")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to clear all pods: {e}")
            return False


class RedisConnectionManager:
    """
    Redis 連線管理器（方便建立 Redis 客戶端）
    """
    
    @staticmethod
    def create_client(
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0,
        redis_password: Optional[str] = None,
        decode_responses: bool = True
    ) -> redis.Redis:
        """
        建立 Redis 客戶端
        
        Args:
            redis_host: Redis 主機
            redis_port: Redis 埠號
            redis_db: Redis 資料庫編號
            redis_password: Redis 密碼
            decode_responses: 是否自動解碼回應為字串
            
        Returns:
            Redis 客戶端實例
        """
        try:
            client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                password=redis_password,
                decode_responses=decode_responses,
                socket_connect_timeout=5,
                socket_keepalive=True,
                health_check_interval=30
            )
            
            # 測試連線
            client.ping()
            logger.info(f"✅ Redis connected: {redis_host}:{redis_port}/{redis_db}")
            return client
        except Exception as e:
            logger.error(f"❌ Redis connection failed: {e}")
            raise


# ============================================
# 使用範例
# ============================================

if __name__ == "__main__":
    """使用範例與測試"""
    
    # 1. 建立 Redis 連線
    redis_client = RedisConnectionManager.create_client(
        redis_host="localhost",
        redis_port=6379,
        redis_db=0
    )
    
    # 2. 單一 Pod 的操作
    print("\n" + "="*60)
    print("單一 Pod 操作範例")
    print("="*60)
    
    pod1 = PodStatusManipulator(redis_client, pod_name="game-pod-001")
    pod1.register()
    pod1.set_idle()
    pod1.set_busy()
    print(f"Pod1 狀態: {pod1.get_status()}")
    
    # 3. 使用 Context Manager
    print("\n" + "="*60)
    print("Context Manager 範例")
    print("="*60)
    
    with PodStatusManipulator(redis_client, "game-pod-002") as pod2:
        pod2.set_idle()
        time.sleep(1)
        pod2.set_busy()
    # 自動註銷
    
    # 4. 監控所有 Pod
    print("\n" + "="*60)
    print("監控所有 Pod")
    print("="*60)
    
    monitor = PodStatusMonitor(redis_client)
    
    # 一次查詢取得所有資料！
    all_pods = monitor.get_all_pods()
    print(f"\n所有 Pods ({len(all_pods)} 個):")
    for name, data in all_pods.items():
        print(f"  - {name}: {data}")
    
    # 統計摘要
    summary = monitor.get_status_summary()
    print("\n狀態摘要:")
    print(f"  總數: {summary['total']}")
    print(f"  空閒: {summary[PodStatus.IDLE]}")
    print(f"  忙碌: {summary[PodStatus.BUSY]}")
    print(f"  錯誤: {summary[PodStatus.ERROR]}")
    print(f"  啟動中: {summary[PodStatus.STARTING]}")
    
    # 查詢特定狀態的 Pod
    idle_pods = monitor.get_idle_pods()
    print(f"\n空閒的 Pods: {idle_pods}")
    
    # 詳細資訊
    details = monitor.get_pods_with_details()
    print("\nPod 詳細資訊:")
    for detail in details:
        print(f"  {detail}")
    
    # 5. 清理過期 Pod
    print("\n" + "="*60)
    print("清理過期 Pod")
    print("="*60)
    
    # 設定一個很短的超時時間來測試（實際使用應該是 600 秒）
    time.sleep(2)
    cleaned = monitor.cleanup_stale_pods(timeout_seconds=1)
    print(f"清理了 {cleaned} 個過期 Pod")
    
    # 6. 清理測試資料
    print("\n" + "="*60)
    print("清理測試資料")
    print("="*60)
    monitor.clear_all()
    print("✅ 測試完成")
