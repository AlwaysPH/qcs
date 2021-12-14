package biz.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.gwi.qcs.core.biz.service.DmQueryService;
import com.gwi.qcs.model.domain.mysql.RrsArea;
import com.gwi.qcs.model.domain.mysql.RrsOrganization;
import com.gwi.qcs.model.mapper.mysql.RrsAreaMapper;
import com.gwi.qcs.model.mapper.mysql.RrsOrganizationMapper;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * DM机构，区域信息查询实现类,查询本地数据库
 *
 * @author: ljl
 * @date: 2021/1/18 16:14
 **/
@Configuration
@ConditionalOnProperty(name = "qcs.dm.type", havingValue = "local")
public class DmLocalQueryServiceImpl implements DmQueryService {

    @Autowired
    private RrsOrganizationMapper rrsOrganizationMapper;

    @Autowired
    private RrsAreaMapper rrsAreaMapper;

    @Override
    public RrsArea findAreaByAreaCode(String areaId) {
        return rrsAreaMapper.findAreaByAreaCode(areaId);
    }

    @Override
    public List<RrsArea> findAreaListByPid(String pid) {
        return rrsAreaMapper.findAreaListByPid(pid);
    }

    @Override
    public List<RrsOrganization> findOrgsByAreaId(String areaId) {
        return rrsOrganizationMapper.findOrgsByAreaId(areaId);
    }

    @Override
    public List<RrsOrganization> findOrgsByAreaCode(String areaId) {
        if(StringUtils.isNotBlank(areaId)){
            areaId = areaId.substring(0,6);
        }
        return rrsOrganizationMapper.findOrgsByAreaCode(areaId);
    }

    @Override
    public IPage<RrsArea> queryAreaPageByDomain(RrsArea areaDomain, Page<RrsArea> page) {
        QueryWrapper<RrsArea> queryWrapper = new QueryWrapper<>();
        queryWrapper.setEntity(areaDomain);
        return rrsAreaMapper.selectPage(page, queryWrapper);
    }

    @Override
    public RrsOrganization getOrgByCode(String orgCode) {
        return rrsOrganizationMapper.getOrg(orgCode);
    }

    @Override
    public IPage<RrsOrganization> queryOrgPageByDomain(RrsOrganization domain, Page page) {
        QueryWrapper<RrsOrganization> queryWrapper = new QueryWrapper<>();
        queryWrapper.setEntity(domain);
        if(page != null){
            return rrsOrganizationMapper.selectPage(page, queryWrapper);
        }
        List<RrsOrganization> rrsOrganizations = rrsOrganizationMapper.selectList(queryWrapper);
        IPage<RrsOrganization> orgsPage = new Page<>();
        if (rrsOrganizations != null) {
            orgsPage.setTotal(rrsOrganizations.size());
            orgsPage.setRecords(rrsOrganizations);
        }
        return orgsPage;
    }
}
