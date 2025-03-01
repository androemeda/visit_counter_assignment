from typing import Dict, Any
import redis
import time
import logging

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
        # Task 1: In-memory counter
        self._visit_counts: Dict[str, int] = {}
        
        # Task 2: Redis connection
        self.redis_client = redis.Redis(
            host='redis1',
            port=6379,
            decode_responses=True
        )
        
        # Task 3: Application cache
        self._cache: Dict[str, Dict[str, Any]] = {}
        self.cache_ttl = 5  # 5 seconds
        
        # Task 4: Write buffer
        self._write_buffer: Dict[str, int] = {}  # {page_id: pending_count}
        self.flush_interval = 30  # 30 seconds
        self.last_flush_time = time.time()
        
        self._initialized = True

    async def increment_visit(self, page_id: str) -> Dict[str, Any]:
        """Increment visit count for a page"""
        try:
            # Check if it's time to flush the buffer
            current_time = time.time()
            if current_time - self.last_flush_time >= self.flush_interval:
                await self._flush_buffer()
                self.last_flush_time = current_time
            
            # Add to write buffer instead of writing directly to Redis
            if page_id not in self._write_buffer:
                self._write_buffer[page_id] = 0
            self._write_buffer[page_id] += 1
            
            # Get current total (Redis + buffer)
            try:
                redis_count = int(self.redis_client.get(f"page:{page_id}") or 0)
            except Exception as e:
                logging.error(f"Redis error: {str(e)}")
                redis_count = 0
                
            buffer_count = self._write_buffer.get(page_id, 0)
            total_count = redis_count + buffer_count
            
            # Update cache with combined count
            self._cache[page_id] = {
                "count": total_count,
                "timestamp": time.time()
            }
            
            return {
                "visits": total_count,
                "served_via": "redis"
            }
            
        except Exception as e:
            logging.error(f"Error in increment_visit: {str(e)}")
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
            
            # Cache miss or expired
            # Also check if we need to flush buffer on this read
            current_time = time.time()
            if current_time - self.last_flush_time >= self.flush_interval:
                await self._flush_buffer()
                self.last_flush_time = current_time
                
            # Get from Redis
            try:
                redis_count = int(self.redis_client.get(f"page:{page_id}") or 0)
            except Exception as e:
                logging.error(f"Redis error: {str(e)}")
                redis_count = 0
            
            # Combine with pending writes in buffer
            buffer_count = self._write_buffer.get(page_id, 0)
            total_count = redis_count + buffer_count
            
            # Update cache
            self._cache[page_id] = {
                "count": total_count,
                "timestamp": time.time()
            }
            
            return {
                "visits": total_count,
                "served_via": "redis"
            }
            
        except Exception as e:
            logging.error(f"Error in get_visit_count: {str(e)}")
            return {"visits": 0, "served_via": "in_memory"}

    async def _flush_buffer(self) -> None:
        """Flush write buffer to Redis"""
        if not self._write_buffer:  # Skip if buffer is empty
            return
            
        try:
            # Copy buffer and clear it
            buffer_to_flush = self._write_buffer.copy()
            self._write_buffer.clear()
            
            # Use pipeline for efficiency
            pipeline = self.redis_client.pipeline()
            for page_id, count in buffer_to_flush.items():
                pipeline.incrby(f"page:{page_id}", count)
            pipeline.execute()
            
            # Update cache with new values
            for page_id in buffer_to_flush:
                if page_id in self._cache:
                    redis_count = int(self.redis_client.get(f"page:{page_id}") or 0)
                    self._cache[page_id]["count"] = redis_count
                    self._cache[page_id]["timestamp"] = time.time()
            
        except Exception as e:
            logging.error(f"Error flushing buffer: {str(e)}")
            # Restore buffer on error
            for page_id, count in buffer_to_flush.items():
                if page_id not in self._write_buffer:
                    self._write_buffer[page_id] = 0
                self._write_buffer[page_id] += count