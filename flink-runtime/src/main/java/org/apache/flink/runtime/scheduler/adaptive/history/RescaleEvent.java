package org.apache.flink.runtime.scheduler.adaptive.history;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

public class RescaleEvent {

    enum Trigger {
        EXCEPTION,
        INIT,
        REST_REQUEST,
        AVAILABLE_RESOURCE
    }

    enum Status {
        COMPLETED,
        FAILED,
        RESCALING
    }

    static class RescaleAttemptID {
        ResourceRequirementsRequestID resourceRequirementsRequestID;
        Integer attempt;
    }

    RescaleAttemptID rescaleAttemptID;
    Trigger trigger;
    Status status;
    String comment;
    Long triggerTimestamp;
    Long endTimestamp;
    Duration duration;
    Map<Object, Object> parallelism;
    Map<Object, Object> slots;

    public RescaleAttemptID getRescaleAttemptID() {
        return rescaleAttemptID;
    }

    public void setRescaleAttemptID(RescaleAttemptID rescaleAttemptID) {
        this.rescaleAttemptID = rescaleAttemptID;
    }

    public Trigger getTrigger() {
        return trigger;
    }

    public void setTrigger(Trigger trigger) {
        this.trigger = trigger;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public Long getTriggerTimestamp() {
        return triggerTimestamp;
    }

    public void setTriggerTimestamp(Long triggerTimestamp) {
        this.triggerTimestamp = triggerTimestamp;
    }

    public Long getEndTimestamp() {
        return endTimestamp;
    }

    public void setEndTimestamp(Long endTimestamp) {
        this.endTimestamp = endTimestamp;
    }

    public Duration getDuration() {
        return duration;
    }

    public void setDuration(Duration duration) {
        this.duration = duration;
    }

    public Map<Object, Object> getParallelism() {
        return parallelism;
    }

    public void setParallelism(Map<Object, Object> parallelism) {
        this.parallelism = parallelism;
    }

    public Map<Object, Object> getSlots() {
        return slots;
    }

    public void setSlots(Map<Object, Object> slots) {
        this.slots = slots;
    }

    @Override
    public String toString() {
        return "RescaleEvent{" +
                "rescaleAttemptID=" + rescaleAttemptID +
                ", trigger=" + trigger +
                ", status=" + status +
                ", comment='" + comment + '\'' +
                ", triggerTimestamp=" + triggerTimestamp +
                ", endTimestamp=" + endTimestamp +
                ", duration=" + duration +
                ", parallelism=" + parallelism +
                ", slots=" + slots +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RescaleEvent that = (RescaleEvent) o;
        return Objects.equals(rescaleAttemptID, that.rescaleAttemptID) && trigger == that.trigger
                && status == that.status && Objects.equals(comment, that.comment) && Objects.equals(
                triggerTimestamp,
                that.triggerTimestamp) && Objects.equals(endTimestamp, that.endTimestamp)
                && Objects.equals(duration, that.duration) && Objects.equals(
                parallelism,
                that.parallelism) && Objects.equals(slots, that.slots);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                rescaleAttemptID,
                trigger,
                status,
                comment,
                triggerTimestamp,
                endTimestamp,
                duration,
                parallelism,
                slots);
    }
}
