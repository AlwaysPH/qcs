package biz.service.impl;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.gwi.qcs.core.biz.service.DmQueryService;
import com.gwi.qcs.model.domain.mysql.RrsArea;
import com.gwi.qcs.model.domain.mysql.RrsOrganization;
import com.gwi.qcs.service.api.ServiceVersion;
import com.hbs.dm.api.OrganizationApi;
import com.hbs.dm.api.RrsAreaApi;
import com.hbs.dm.facade.DmFacadeApi;
import com.hbs.dm.rrs.entity.RRSOrganization;
import com.hbs.dm.rrs.response.ResponseSelect;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.dubbo.config.annotation.Reference;
import org.springframework.beans.BeanUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * DM机构，区域信息查询实现类，通过dubbo接口获取
 *
 * @author: ljl
 * @date: 2021/1/18 16:14
 **/
@Configuration
@ConditionalOnProperty(name = "qcs.dm.type", havingValue = "dubbo")
public class DmDubboQueryServiceImpl implements DmQueryService {

    @Reference(version = ServiceVersion.ORGANIZATION_API)
    private OrganizationApi organizationApi;

    @Reference(version = ServiceVersion.ORGANIZATION_API)
    private RrsAreaApi rrsAreaApi;

    @Reference(version = ServiceVersion.ORGANIZATION_API)
    private DmFacadeApi dmFacadeApi;

    @Override
    public RrsArea findAreaByAreaCode(String areaId) {
        com.hbs.dm.rrs.entity.RrsArea rrsArea = dmFacadeApi.findAreaByAreaCode(areaId);
        RrsArea rrsArea1 = null;
        if(rrsArea != null){
            rrsArea1 = new RrsArea();
            BeanUtils.copyProperties(rrsArea, rrsArea1);
        }
        return rrsArea1;
    }

    @Override
    public List<RrsOrganization> findOrgsByAreaCode(String areaId) {
        return toRrsOrganizationList(dmFacadeApi.findOrgsByAreaId(areaId));
    }

    @Override
    public List<RrsArea> findAreaListByPid(String pid) {
        List<com.hbs.dm.rrs.entity.RrsArea> rrsAreaList = dmFacadeApi.findareaListByPid(pid);
        return toList(rrsAreaList);
    }

    @Override
    public List<RrsOrganization> findOrgsByAreaId(String areaId) {
        return toRrsOrganizationList(dmFacadeApi.findOrgsByAreaId(areaId));
    }

    @Override
    public IPage<RrsArea> queryAreaPageByDomain(RrsArea rrsArea, Page<RrsArea> page) {
        rrsArea.setStart((int) (page.getSize() * (page.getCurrent() - 1)));
        rrsArea.setRows((int) page.getSize());
        com.hbs.dm.rrs.entity.RrsArea param = new com.hbs.dm.rrs.entity.RrsArea();
        BeanUtils.copyProperties(rrsArea, param);
        ResponseSelect<com.hbs.dm.rrs.entity.RrsArea> rrsAreaResp = rrsAreaApi.selectAreaPage(param);

        IPage<RrsArea> areaPage = new Page<>();
        if (rrsAreaResp != null) {
            areaPage.setTotal(rrsAreaResp.getTotal());
            areaPage.setRecords(toList(rrsAreaResp.getList()));
        }
        return areaPage;
    }

    @Override
    public RrsOrganization getOrgByCode(String orgCode) {
        RRSOrganization rrsOrganization = organizationApi.getOrg(orgCode);
        RrsOrganization rrsOrganization1 = null;
        if(rrsOrganization != null){
            rrsOrganization1 = new RrsOrganization();
            BeanUtils.copyProperties(rrsOrganization, rrsOrganization1);
        }
        return rrsOrganization1;
    }

    @Override
    public IPage<RrsOrganization> queryOrgPageByDomain(RrsOrganization domain, Page page) {
        if(page != null){
            domain.setStart((int) (page.getSize() * (page.getCurrent() - 1)));
            domain.setRows((int) page.getSize());
        }
        RRSOrganization rrsOrganization = new RRSOrganization();
        BeanUtils.copyProperties(domain, rrsOrganization);
        ResponseSelect<RRSOrganization> orgResp = organizationApi.selectOragPage(rrsOrganization);

        IPage<RrsOrganization> areaPage = new Page<>();
        if (orgResp != null) {
            areaPage.setTotal(orgResp.getTotal());
            areaPage.setRecords(toRrsOrganizationList(orgResp.getList()));
        }
        return areaPage;
    }

    List<RrsArea> toList(List<com.hbs.dm.rrs.entity.RrsArea> rrsAreaList){
        List<RrsArea> areaList = new ArrayList<>();
        if(CollectionUtils.isNotEmpty(rrsAreaList)){
            for(com.hbs.dm.rrs.entity.RrsArea rrsArea1 : rrsAreaList){
                RrsArea rrsArea2 = new RrsArea();
                BeanUtils.copyProperties(rrsArea1, rrsArea2);
                areaList.add(rrsArea2);
            }
        }
        return areaList;
    }

    List<RrsOrganization> toRrsOrganizationList(List<RRSOrganization> rrsOrganizationList){
        List<RrsOrganization> list = new ArrayList<>();
        if(CollectionUtils.isNotEmpty(rrsOrganizationList)){
            for(RRSOrganization rrsOrganization : rrsOrganizationList){
                RrsOrganization rrsArea2 = new RrsOrganization();
                BeanUtils.copyProperties(rrsOrganization, rrsArea2);
                list.add(rrsArea2);
            }
        }
        return list;
    }
}
