from typing import Dict, Any
import redis
import time
import logging
import hashlib
from bisect import bisect

class ConsistentHash:
    def __init__(self, nodes: list, virtual_nodes: int = 100):
        """Initialize the consistent hash ring"""
        self.virtual_nodes = virtual_nodes
        self.hash_ring = {}
        self.sorted_keys = []
        
        for node in nodes:
            self.add_node(node)
    
    def _get_hash(self, key: str) -> int:
        """Generate hash for a key"""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    def add_node(self, node: str) -> None:
        """Add a node to the hash ring"""
        for i in range(self.virtual_nodes):
            virtual_node = f"{node}_{i}"
            hash_value = self._get_hash(virtual_node)
            self.hash_ring[hash_value] = node
            self.sorted_keys.append(hash_value)
        self.sorted_keys.sort()
    
    def remove_node(self, node: str) -> None:
        """Remove a node from the hash ring"""
        for i in range(self.virtual_nodes):
            virtual_node = f"{node}_{i}"
            hash_value = self._get_hash(virtual_node)
            if hash_value in self.hash_ring:
                del self.hash_ring[hash_value]
                self.sorted_keys.remove(hash_value)
    
    def get_node(self, key: str) -> str:
        """Get the node responsible for the given key"""
        if not self.hash_ring:
            raise Exception("Hash ring is empty")
        
        key_hash = self._get_hash(key)
        index = bisect(self.sorted_keys, key_hash)
        
        # Wrap around to first node if past the end
        if index >= len(self.sorted_keys):
            index = 0
            
        return self.hash_ring[self.sorted_keys[index]]


class VisitCounterService:
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        """Initialize the visit counter service"""
        # Task 5: Redis sharding setup
        self.redis_nodes = {
            'redis_7070': redis.Redis(host='redis1', port=7070, decode_responses=True),
            'redis_7071': redis.Redis(host='redis2', port=7071, decode_responses=True)
        }
        
        # Initialize consistent hashing
        self.consistent_hash = ConsistentHash(['redis_7070', 'redis_7071'])
        
        # Task 3: Application cache
        self._cache: Dict[str, Dict[str, Any]] = {}
        self.cache_ttl = 5  # 5 seconds
        
        # Task 4: Write buffer
        self._write_buffer: Dict[str, Dict[str, int]] = {
            'redis_7070': {},
            'redis_7071': {}
        }
        self.flush_interval = 30  # 30 seconds
        self.last_flush_time = time.time()
        
        self._initialized = True

    def _get_redis_client(self, page_id: str) -> tuple:
        """Get the appropriate Redis client and node name for a page_id"""
        node = self.consistent_hash.get_node(page_id)
        return self.redis_nodes[node], node

    async def increment_visit(self, page_id: str) -> Dict[str, Any]:
        """Increment visit count for a page"""
        try:
            # Check if it's time to flush the buffer
            current_time = time.time()
            if current_time - self.last_flush_time >= self.flush_interval:
                await self._flush_buffer()
                self.last_flush_time = current_time
            
            # Determine which shard to use
            redis_client, node = self._get_redis_client(page_id)
            
            # Add to write buffer
            if page_id not in self._write_buffer[node]:
                self._write_buffer[node][page_id] = 0
            self._write_buffer[node][page_id] += 1
            
            # Get current count from Redis
            try:
                redis_count = int(redis_client.get(f"page:{page_id}") or 0)
            except Exception as e:
                logging.error(f"Redis error: {str(e)}")
                redis_count = 0
            
            # Calculate total (Redis + buffer)
            buffer_count = self._write_buffer[node].get(page_id, 0)
            total_count = redis_count + buffer_count
            
            # Update cache
            self._cache[page_id] = {
                "count": total_count,
                "timestamp": time.time(),
                "node": node
            }
            
            return {
                "visits": total_count,
                "served_via": node
            }
            
        except Exception as e:
            logging.error(f"Error in increment_visit: {str(e)}")
            # Fallback to cache or 0
            if page_id in self._cache:
                return {
                    "visits": self._cache[page_id]["count"],
                    "served_via": "in_memory"
                }
            return {"visits": 0, "served_via": "in_memory"}

    async def get_visit_count(self, page_id: str) -> Dict[str, Any]:
        """Get visit count for a page"""
        try:
            # Check cache first
            if page_id in self._cache:
                cache_entry = self._cache[page_id]
                current_time = time.time()
                
                # If cache is still valid
                if current_time - cache_entry["timestamp"] < self.cache_ttl:
                    return {
                        "visits": cache_entry["count"],
                        "served_via": "in_memory"
                    }
            
            # Cache miss or expired, get from Redis
            redis_client, node = self._get_redis_client(page_id)
            
            # Check if we need to flush on this read
            current_time = time.time()
            if current_time - self.last_flush_time >= self.flush_interval:
                await self._flush_buffer()
                self.last_flush_time = current_time
                
            # Get from Redis
            try:
                redis_count = int(redis_client.get(f"page:{page_id}") or 0)
            except Exception as e:
                logging.error(f"Redis error: {str(e)}")
                # If cache exists but expired, still use it as fallback
                if page_id in self._cache:
                    return {
                        "visits": self._cache[page_id]["count"],
                        "served_via": "in_memory"
                    }
                redis_count = 0
            
            # Get from buffer
            buffer_count = self._write_buffer[node].get(page_id, 0)
            total_count = redis_count + buffer_count
            
            # Update cache
            self._cache[page_id] = {
                "count": total_count,
                "timestamp": time.time(),
                "node": node
            }
            
            return {
                "visits": total_count,
                "served_via": node
            }
            
        except Exception as e:
            logging.error(f"Error in get_visit_count: {str(e)}")
            # Fallback to cache or 0
            if page_id in self._cache:
                return {
                    "visits": self._cache[page_id]["count"],
                    "served_via": "in_memory"
                }
            return {"visits": 0, "served_via": "in_memory"}

    async def _flush_buffer(self) -> None:
        """Flush write buffer to Redis"""
        for node, buffer in self._write_buffer.items():
            if not buffer:  # Skip empty buffers
                continue
                
            try:
                # Copy buffer and clear it
                buffer_to_flush = buffer.copy()
                buffer.clear()
                
                # Use pipeline for efficiency
                redis_client = self.redis_nodes[node]
                pipeline = redis_client.pipeline()
                for page_id, count in buffer_to_flush.items():
                    pipeline.incrby(f"page:{page_id}", count)
                pipeline.execute()
                
                # Update cache with new values
                for page_id in buffer_to_flush:
                    if page_id in self._cache:
                        redis_count = int(redis_client.get(f"page:{page_id}") or 0)
                        self._cache[page_id]["count"] = redis_count
                        self._cache[page_id]["timestamp"] = time.time()
                
            except Exception as e:
                logging.error(f"Error flushing buffer for {node}: {str(e)}")
                # Restore buffer on error
                for page_id, count in buffer_to_flush.items():
                    if page_id not in buffer:
                        buffer[page_id] = 0
                    buffer[page_id] += count