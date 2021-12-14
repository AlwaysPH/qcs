package biz.service.dao.mysql;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.google.common.collect.Maps;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.common.exception.BizException;
import com.gwi.qcs.core.biz.schedule.QuartzJobManager;
import com.gwi.qcs.core.biz.schedule.exception.data.DataExceptionJobAbstractQueryExceptionJob;
import com.gwi.qcs.core.biz.schedule.exception.org.OrgExceptionJobAbstractQueryExceptionJob;
import com.gwi.qcs.model.domain.mysql.Parameter;
import com.gwi.qcs.model.mapper.mysql.ParameterMapper;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;

@Service
@DS(DbTypeConstant.MYSQL)
public class ParameterService extends SuperServiceImpl<ParameterMapper, Parameter> {

    /**
     * 异常数据表定时任务
     */
    private static final String SCHEDULE_EXCEPTION_DATA_CRON = "schedule_exception_dataCron";

    /**
     * 异常机构表定时任务
     */
    private static final String SCHEDULE_EXCEPTION_ORG_CRON = "schedule_exception_orgCron";

    /**
     * 查询ck异常明细最大数
     */
    private static final String QUERY_CK_MAX_ERROR_SIZE = "query_ck_max_error_size";

    /**
     * 资源ID键值对
     */
    private static final String SOURCE_ID_JSON = "source_id_json";

    public Parameter getParameterById(Long id) {
        return baseMapper.selectById(id);
    }

    @Transactional
    public String addParameter(Parameter parameter) {
        Parameter toParameter = new Parameter();
        toParameter.setCode(parameter.getCode());
        Wrapper<Parameter> queryWrapper = new QueryWrapper<>();
        ((QueryWrapper<Parameter>) queryWrapper).setEntity(toParameter);
        int count = baseMapper.selectCount(queryWrapper);
        if (count > 0) {
            throw new BizException("参数编号重复，请修改后保存！");
        }
        baseMapper.insert(parameter);
        return parameter.getId();
    }

    /**
     * 修改参数
     *
     * @param parameter
     * @return
     */
    public int updateParam(Parameter parameter) throws Exception{
        int result = baseMapper.updateById(parameter);
        if(result > 0){
            String code = parameter.getCode();
            if(SCHEDULE_EXCEPTION_DATA_CRON.equals(code)
            || SCHEDULE_EXCEPTION_ORG_CRON.equals(code)){
                initCron(code, parameter.getStatus() == 0, parameter.getValue());
            }
        }
        return result;
    }


    /**
     * 删除参数
     *
     * @param toParameter
     * @return
     */
    @Transactional
    public int deleteParam(Parameter toParameter) {
        return baseMapper.deleteById(toParameter.getId());
    }

    /**
     * 查询参数列表
     *
     * @param toParameter
     * @return
     */
    public List<Parameter> getParamList(Parameter toParameter) {
        Wrapper<Parameter> queryWrapper = new QueryWrapper<>();
        ((QueryWrapper<Parameter>) queryWrapper).setEntity(toParameter);
        return baseMapper.selectList(queryWrapper);
    }

    public List<Parameter> getByCodePrefix(String prefix) {
        QueryWrapper<Parameter> wrapper = new QueryWrapper<>();
        wrapper.like("CODE", prefix + "%");
        wrapper.like("STATUS", 1);
        return baseMapper.selectList(wrapper);
    }

    public IPage<Parameter> page(Parameter toParameter, Integer pageIndex, Integer pageSize) {
        Page<Parameter> page = new Page<>(pageIndex, pageSize);
        Wrapper<Parameter> queryWrapper = new QueryWrapper<>();
        ((QueryWrapper<Parameter>) queryWrapper).setEntity(toParameter);
		((QueryWrapper<Parameter>) queryWrapper).orderByDesc("CREATE_AT");
        return baseMapper.selectPage(page, queryWrapper);
    }

    public Parameter getByCode(String key) {
        QueryWrapper<Parameter> wrapper = new QueryWrapper<>();
        wrapper.eq("CODE", key);
        wrapper.eq("STATUS", 0);
        return baseMapper.selectOne(wrapper);
    }

    /**
     * 获取导出最大条数
     * @return
     */
    public Integer getMaxExportNum(){
        Parameter parameter = new Parameter();
        parameter.setCode("MAX_EXPORT_NUM");
        QueryWrapper<Parameter> queryWrapper = new QueryWrapper<>();
        queryWrapper.setEntity(parameter);
        return Integer.valueOf(baseMapper.selectOne(queryWrapper).getValue());
    }

    /**
     * 获取导出最大条数
     * @return
     */
    public Integer getMaxErrorSize(){
        Parameter parameter = new Parameter();
        parameter.setCode(QUERY_CK_MAX_ERROR_SIZE);
        QueryWrapper<Parameter> queryWrapper = new QueryWrapper<>();
        queryWrapper.setEntity(parameter);
        return Integer.valueOf(baseMapper.selectOne(queryWrapper).getValue());
    }

    /**
     * 根据编码获取值
     * @return
     */
    public String getParameterByCode(String code){
        QueryWrapper<Parameter> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("CODE", code);
        return baseMapper.selectOne(queryWrapper).getValue();
    }

    public void initSchedule() throws Exception{
        initCron(SCHEDULE_EXCEPTION_ORG_CRON, true, null);
    }

    protected void initCron(String code, boolean isOn, String cron) throws Exception{
        String id = CommonConstant.ORG_EXCEPTION_JOB_ID;
        Class clazz = OrgExceptionJobAbstractQueryExceptionJob.class;
        if(SCHEDULE_EXCEPTION_DATA_CRON.equals(code)){
            id = CommonConstant.DATA_EXCEPTION_JOB_ID;
            clazz = DataExceptionJobAbstractQueryExceptionJob.class;
        }
        QuartzJobManager quartzJobManager = QuartzJobManager.getInstance();
        quartzJobManager.deleteJob(id, CommonConstant.SCHEDULE);
        if(isOn){
            quartzJobManager.addJob(clazz, id, CommonConstant.SCHEDULE, StringUtils.isEmpty(cron) ?
                this.getParameterByCode(code) : cron);
        }
    }

    /**
     * 获取sourceMap
     * @param isGetName true为名称，false为机构编码
     * @return
     */
    public Map<String, String> getSourceIdMap(boolean isGetName){
        String value = this.getParameterByCode(SOURCE_ID_JSON);
        String[] valueArr = value.split(",");
        Map<String, String> map = Maps.newHashMap();
        for(String item : valueArr){
            String[] str = item.split(StringUtils.SPACE);
            map.put(str[0], isGetName ? str[1] : str[2]);
        }
        return map;
    }
}
