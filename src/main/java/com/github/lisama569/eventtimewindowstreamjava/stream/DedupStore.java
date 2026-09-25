package com.github.lisama569.eventtimewindowstreamjava.stream;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 有界去重存储：记录已处理的事件 ID，容量达到上限时按插入顺序淘汰最旧条目，
 * 保证长期运行不出现无界状态增长。线程安全。
 */
public class DedupStore {

    private final int maxEntries;
    private final Map<String, Boolean> seen;

    public DedupStore(int maxEntries) {
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be positive");
        }
        this.maxEntries = maxEntries;
        this.seen = new LinkedHashMap<>(16, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > DedupStore.this.maxEntries;
            }
        };
    }

    /** 仅检查 ID 是否已存在，不做记录。 */
    public synchronized boolean contains(String eventId) {
        return seen.containsKey(eventId);
    }

    /** 若 ID 已存在返回 false（重复），否则记录并返回 true。 */
    public synchronized boolean markIfAbsent(String eventId) {
        if (seen.containsKey(eventId)) {
            return false;
        }
        seen.put(eventId, Boolean.TRUE);
        return true;
    }

    public synchronized int size() {
        return seen.size();
    }
}
