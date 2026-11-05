package build.buildfarm.actioncache;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache<K, V> extends LinkedHashMap<K, V> {
    private final int capacity;

    public LRUCache(int capacity) {
        // initialCapacity, loadFactor, accessOrder = true (enables LRU behavior)
        super(capacity, 0.75f, true);
        this.capacity = capacity;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        // Evicts the eldest entry automatically when the size exceeds capacity
        return size() > capacity;
    }

    // Thread-safe wrapper (optional)
    public static <K, V> Map<K, V> createSynchronizedCache(int capacity) {
        return java.util.Collections.synchronizedMap(new LRUCache<>(capacity));
    }
}
