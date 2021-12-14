package biz.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.gwi.qcs.model.domain.mysql.RrsArea;
import com.gwi.qcs.model.domain.mysql.RrsOrganization;

import java.util.List;

/**
 * @description: DM中区域和机构信息查询
 * @author: ljl
 * @date: 2021/1/18 16:11
 **/
public interface DmQueryService {

    /**
     * 查询区域列表
     *
     * @return
     */
    RrsArea findAreaByAreaCode(String areaId);

    /**
     * 查询区域列表
     *
     * @return
     */
    List<RrsArea> findAreaListByPid(String pid);

    /**
     * 根据区域ID查询机构列表
     *
     * @return
     */
    List<RrsOrganization> findOrgsByAreaId(String areaId);


    /**
     * 根据区域ID查询机构列表v2
     *
     * @return
     */
    List<RrsOrganization> findOrgsByAreaCode(String areaId);

    /**
     * 查询区域列表
     *
     * @return
     */
    IPage<RrsArea> queryAreaPageByDomain(RrsArea domain, Page<RrsArea> page);

    /**
     * 查询机构列表
     *
     * @return
     */
    IPage<RrsOrganization> queryOrgPageByDomain(RrsOrganization domain, Page<RrsOrganization> page);

    /**
     * 查询机构code
     *
     * @return
     */
    RrsOrganization getOrgByCode(String orgCode);
}
