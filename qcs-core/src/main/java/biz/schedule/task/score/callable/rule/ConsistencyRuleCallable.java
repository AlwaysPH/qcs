package biz.schedule.task.score.callable.rule;

import com.gwi.qcs.core.biz.schedule.task.score.callable.ParentScoreCallable;
import com.gwi.qcs.model.entity.ScoreEntity;
import com.gwi.qcs.model.mapenum.RecordDataEnum;

import java.util.Map;

public class ConsistencyRuleCallable extends ParentScoreCallable {

    public ConsistencyRuleCallable(ScoreEntity scoreEntity) {
        super(scoreEntity, RecordDataEnum.RECORD_QUALITY_CONSISTENCY);
    }

    @Override
    public Map<String, Object> call() throws Exception {
        return super.call();
    }

}
