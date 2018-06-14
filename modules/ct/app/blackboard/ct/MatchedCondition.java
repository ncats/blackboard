package blackboard.ct;

public class MatchedCondition {
    final public Float score;
    final public Condition condition;
    protected MatchedCondition (Float score, Condition condition) {
        this.score = score;
        this.condition = condition;
    }
}
