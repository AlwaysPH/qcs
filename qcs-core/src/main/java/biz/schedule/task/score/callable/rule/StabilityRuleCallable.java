package biz.schedule.task.score.callable.rule;

import com.gwi.qcs.core.biz.schedule.task.score.callable.ParentScoreCallable;
import com.gwi.qcs.model.entity.ScoreEntity;
import com.gwi.qcs.model.mapenum.RecordDataEnum;

import java.util.Map;

/**
 * 稳定性评分线程类
 *
 * @author penghong
 */
public class StabilityRuleCallable extends ParentScoreCallable {

    public StabilityRuleCallable(ScoreEntity scoreEntity) {
        super(scoreEntity, RecordDataEnum.RECORD_QUALITY_STABILITY);
    }

    @Override
    public Map<String, Object> call() throws Exception {
        return super.call();
    }
}
