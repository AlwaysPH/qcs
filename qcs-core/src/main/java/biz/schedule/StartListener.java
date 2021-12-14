package biz.schedule;

import com.greatwall.component.ccyl.redis.template.RedisRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Set;

/***
 * 监听项目启动，删除redis中所有任务状态key值，防止服务重启，任务无法运行
 */
@Component
public class StartListener implements ApplicationRunner {

    private static final String JOB_STATUS_KEY_PREFIX = "JOB_STATUS_";

    @Autowired
    protected RedisRepository redisRepository;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        Set<String> sets = redisRepository.keys(JOB_STATUS_KEY_PREFIX);
        sets.stream().forEach(set -> {
            redisRepository.del(set);
        });
    }
}
