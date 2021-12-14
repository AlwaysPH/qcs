package biz.service.dao.mysql;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.greatwall.component.ccyl.common.service.impl.SuperServiceImpl;
import com.gwi.qcs.common.constant.CommonConstant;
import com.gwi.qcs.common.constant.DbTypeConstant;
import com.gwi.qcs.model.domain.mysql.DatasetField;
import com.gwi.qcs.model.mapper.mysql.DatasetFieldMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author yanhan
 * @create 2020-08-06 11:27
 **/
@Slf4j
@Service
@DS(DbTypeConstant.MYSQL)
public class DatasetFiledService extends SuperServiceImpl<DatasetFieldMapper, DatasetField> {

    @Cacheable(keyGenerator = "keyGenerator", value = CommonConstant.REDIS_CACHE_STANDARD, unless = "#result == null")
    public String getCodeById(String id) {
        return getString(id, false);
    }

    @Cacheable(keyGenerator = "keyGenerator", value = CommonConstant.REDIS_CACHE_STANDARD, unless = "#result == null")
    public String getNameById(String id) {
        return getString(id, true);
    }

    @Cacheable(keyGenerator = "keyGenerator", value = CommonConstant.REDIS_CACHE_STANDARD, unless = "#result == null")
    public String getTypeOrDesById(String id, boolean isType) {
        DatasetField datasetField = this.getById(id);
        if (datasetField == null) {
            return CommonConstant.DASH;
        } else {
            return isType ? datasetField.getMetadataType() : datasetField.getElementDesc();
        }
    }

    @Cacheable(keyGenerator = "keyGenerator", value = CommonConstant.REDIS_CACHE_STANDARD, unless = "#result == null")
    public DatasetField getByField(long fieldId) {
        DatasetField datasetField = new DatasetField();
        datasetField.setId(fieldId);
        return this.getOne(new QueryWrapper<>(datasetField));
    }

    private String getString(String id, boolean isName) {
        DatasetField datasetField = this.getById(id);
        if (datasetField == null) {
            return CommonConstant.DASH;
        } else {
            return isName ? datasetField.getElementName() : datasetField.getFieldName();
        }
    }

    public Map<String, String> getKeysByTableName(String tableName) {
        DatasetField datasetField = new DatasetField();
        datasetField.setSourceType(CommonConstant.SOURCE_TYPE_INSTANCE);
        datasetField.setMetasetCode(tableName);
        datasetField.setIsKey(CommonConstant.FIELD_STATUS_TRUE);
        List<DatasetField> datasetFieldList = this.list(new QueryWrapper<>(datasetField));
        return datasetFieldList.stream().collect(Collectors.toMap(DatasetField::getFieldName, DatasetField::getMetadataType));
    }

    public Map<String, String> getMetaSetAndMetDataForFieldNameMap(Long standardId, String sourceType) {
        DatasetField query = new DatasetField();
        query.setStandardId(standardId);
        query.setSourceType(sourceType);
        return this.list(new QueryWrapper<>(query)).stream()
                .collect(Collectors.toMap(datasetField -> datasetField.getMetasetCode() + datasetField.getMetadataCode(),
                        DatasetField::getFieldName));
    }

    public List<DatasetField> getByStandardId(Long standardId, String sourceType) {
        DatasetField datasetField = new DatasetField();
        datasetField.setStandardId(standardId);
        datasetField.setSourceType(sourceType);
        QueryWrapper<DatasetField> queryWrapper = new QueryWrapper<>(datasetField);
        return this.list(queryWrapper);
    }

}
