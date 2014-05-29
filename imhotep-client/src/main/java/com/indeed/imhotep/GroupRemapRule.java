package com.indeed.imhotep;

public class GroupRemapRule {
    public final int targetGroup;
    public final RegroupCondition condition;
    public final int negativeGroup;
    public final int positiveGroup;

    public GroupRemapRule(int targetGroup, RegroupCondition condition, int negativeGroup, int positiveGroup) {
        this.targetGroup = targetGroup;
        this.condition = condition;
        this.negativeGroup = negativeGroup;
        this.positiveGroup = positiveGroup;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GroupRemapRule that = (GroupRemapRule) o;

        if (negativeGroup != that.negativeGroup) return false;
        if (positiveGroup != that.positiveGroup) return false;
        if (targetGroup != that.targetGroup) return false;
        if (condition != null ? !condition.equals(that.condition) : that.condition != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = targetGroup;
        result = 31 * result + (condition != null ? condition.hashCode() : 0);
        result = 31 * result + negativeGroup;
        result = 31 * result + positiveGroup;
        return result;
    }
}
