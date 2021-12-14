package biz.schedule.task.score.callable.rule;

import com.gwi.qcs.core.biz.schedule.task.score.callable.ParentScoreCallable;
import com.gwi.qcs.model.entity.ScoreEntity;
import com.gwi.qcs.model.mapenum.RecordDataEnum;

import java.util.Map;

public class TimelinesRuleCallable extends ParentScoreCallable {

    public TimelinesRuleCallable(ScoreEntity scoreEntity) {
        super(scoreEntity, RecordDataEnum.RECORD_QUALITY_TIMELINES);
    }

    @Override
    public Map<String, Object> call() throws Exception {
        return super.call();
    }

}
