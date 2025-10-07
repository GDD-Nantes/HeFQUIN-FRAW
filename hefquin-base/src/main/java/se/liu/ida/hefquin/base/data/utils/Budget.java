package se.liu.ida.hefquin.base.data.utils;

import java.util.Objects;

public class Budget {
    private Integer attempts = null;
    private Integer remoteAttempts = null;
    private Integer limit = null;
    private Integer timeout = null;

    private final static Integer DEFAULT_VALUE = Integer.MAX_VALUE;

    // ---------- BUILDER ----------

    public Budget setAttempts(Integer attempts) {
        this.attempts = attempts;
        return this;
    }

    public Budget setRemoteAttempts(Integer remoteAttempts) {
        this.remoteAttempts = remoteAttempts;
        return this;
    }

    public Budget setLimit(Integer limit) {
        this.limit = limit;
        return this;
    }

    public Budget setTimeout(Integer timeout) {
        this.timeout = timeout;
        return this;
    }

    // ---------- GETTER METHODS ----------

    public Integer getAttempts() {
        return Objects.requireNonNullElse(attempts, DEFAULT_VALUE);
    }

    public Integer getRemoteAttempts() {
        return Objects.requireNonNullElse(remoteAttempts, DEFAULT_VALUE);
    }

    public Integer getLimit() {
        return Objects.requireNonNullElse(limit, DEFAULT_VALUE);
    }

    public Integer getTimeout() {
        return Objects.requireNonNullElse(timeout, DEFAULT_VALUE);
    }


    // ---------- HELPER METHODS ----------

    public Budget fillWith(Budget other) {
        if(this.attempts == null) this.attempts = other.attempts;
        if(this.remoteAttempts == null) this.remoteAttempts = other.remoteAttempts;
        if(this.limit == null) this.limit = other.limit;
        if(this.timeout == null) this.timeout = other.timeout;
        return this;
    }

    /**
     * Creates a "minimum"-ed budget out of two budgets
     * @param budget1 the first budget to merge (order doesn't matter)
     * @param budget2 the second budget to merge (order doesn't matter)
     * @return a new Budget object. All fields' value are the minimum value from budget 1 and 2 for that field.
     */
    public static Budget mergeMin(Budget budget1, Budget budget2) {
        return new Budget()
                .setAttempts(Math.min(budget1.getAttempts(), budget2.getAttempts()))
                .setRemoteAttempts(Math.min(budget1.getRemoteAttempts(), budget2.getRemoteAttempts()))
                .setLimit(Math.min(budget1.getLimit(), budget2.getLimit()))
                .setTimeout(Math.min(budget1.getTimeout(), budget2.getTimeout()));
    }

    /**
     * Creates a "maximum"-ed budget out of two budgets
     * @param budget1 the first budget to merge (order doesn't matter)
     * @param budget2 the second budget to merge (order doesn't matter)
     * @return a new Budget object. All fields' value are the maximum value from budget 1 and 2 for that field.
     */
    public static Budget mergeMax(Budget budget1, Budget budget2) {
        return new Budget()
                .setAttempts(Math.max(budget1.getAttempts(), budget2.getAttempts()))
                .setRemoteAttempts(Math.max(budget1.getRemoteAttempts(), budget2.getRemoteAttempts()))
                .setLimit(Math.max(budget1.getLimit(), budget2.getLimit()))
                .setTimeout(Math.max(budget1.getTimeout(), budget2.getTimeout()));
    }

    @Override
    public String toString() {
        return "attempts=" + attempts +
                "\n, remoteAttempts=" + remoteAttempts +
                "\n, limit=" + limit +
                "\n, timeout=" + timeout;
    }
}
