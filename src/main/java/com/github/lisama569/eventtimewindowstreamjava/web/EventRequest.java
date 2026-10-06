package com.github.lisama569.eventtimewindowstreamjava.web;

/**
 * 事件接入请求体（字段与 {@link com.github.lisama569.eventtimewindowstreamjava.stream.Event} 对应）。
 */
public class EventRequest {

    private String eventId;
    private String tenant;
    private int partition;
    private long eventTimeMillis;
    private long amount;

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getEventTimeMillis() {
        return eventTimeMillis;
    }

    public void setEventTimeMillis(long eventTimeMillis) {
        this.eventTimeMillis = eventTimeMillis;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }
}
