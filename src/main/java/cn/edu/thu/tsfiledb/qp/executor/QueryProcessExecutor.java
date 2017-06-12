package cn.edu.thu.tsfiledb.qp.executor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;

import cn.edu.thu.tsfiledb.auth.dao.Authorizer;
import cn.edu.thu.tsfiledb.auth.model.AuthException;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.metadata.MManager;
import cn.edu.thu.tsfiledb.qp.constant.SQLConstant;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.logical.operator.Operator;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;
import cn.edu.thu.tsfiledb.qp.strategy.Transformer;

public abstract class QueryProcessExecutor {
    protected final boolean isSingleFile;

    protected ThreadLocal<Map<String, Object>> parameters = new ThreadLocal<>();
    protected int fetchSize = 100;

    public QueryProcessExecutor(boolean isSingleFile) {
        this.isSingleFile = isSingleFile;
    }

    protected abstract TSDataType getNonReservedSeriesType(Path fullPath) throws PathErrorException;

    protected abstract boolean judgeNonReservedPathExists(Path fullPath);

    public PhysicalPlan transformToPhysicalPlan(Operator operator) throws QueryProcessorException {
        Transformer transformer = new Transformer(this);
        return transformer.transformToPhysicalPlan(operator);
    }

    public boolean isSingleFile() {
        return isSingleFile;
    }

    public TSDataType getSeriesType(Path fullPath) throws PathErrorException {
        if (fullPath.equals(SQLConstant.RESERVED_TIME))
            return TSDataType.INT64;
        if (fullPath.equals(SQLConstant.RESERVED_FREQ))
            return TSDataType.FLOAT;
        return getNonReservedSeriesType(fullPath);
    }

    public boolean judgePathExists(Path pathStr) {
        return SQLConstant.isReservedPath(pathStr) || judgeNonReservedPathExists(pathStr);
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public abstract QueryDataSet query(List<Path> paths, FilterExpression timeFilter,
            FilterExpression freqFilter, FilterExpression valueFilter, int fetchSize,
            QueryDataSet lastData) throws ProcessorException;

    /**
     * execute update command and return whether the operator is successful.
     * 
     * @param path : update series path
     * @param startTime
     * @param endTime
     * @param value - in type of string
     * @return - whether the operator is successful.
     */
    public abstract boolean update(Path path, long startTime, long endTime, String value) throws ProcessorException;
    
    /**
     * execute delete command and return whether the operator is successful.
     * 
     * @param path : delete series path
     * @param deleteTime
     * @return - whether the operator is successful.
     */
    public abstract boolean delete(Path path, long deleteTime) throws ProcessorException;
    
    /**
     * execute insert command and return whether the operator is successful.
     * 
     * @param path
     * @param insertTime - it's time point but not a range
     * @param value
     * @return - Operate Type.
     */
    public abstract int insert(Path path, long insertTime, String value) throws ProcessorException;
    
    public abstract int multiInsert(String deltaObject, long insertTime,
    		List<String> measurementList, List<String> insertValues) throws ProcessorException;

    public MManager getMManager() {
        return MManager.getInstance();
    }

    public void addParameter(String key, Object value) {
    	if(parameters.get() == null){
    		parameters.set(new HashMap<>());
    	}
        parameters.get().put(key, value);
    }

    public Object getParameter(String key) {
        return parameters.get().get(key);
    }

    public void clearParameters(){
    	if(parameters.get() != null){
    		parameters.get().clear();
    	}
    }

    /**
     *
     * @param username updated user's name
     * @param newPassword new password
     * @return boolean
     * @throws AuthException
     */
    public boolean updateUser(String username,String newPassword) throws AuthException{
    	return Authorizer.updateUserPassword(username, newPassword);
    }
    
    public boolean createUser(String username, String password) throws AuthException {
        return Authorizer.createUser(username, password);
    }

    public boolean addPermissionToUser(String userName, String nodeName, int permissionId)
            throws AuthException {
        return Authorizer.addPmsToUser(userName, nodeName, permissionId);
    }

    public boolean removePermissionFromUser(String userName, String nodeName, int permissionId)
            throws AuthException {
        return Authorizer.removePmsFromUser(userName, nodeName, permissionId);
    }


    public boolean deleteUser(String userName) throws AuthException {
        return Authorizer.deleteUser(userName);
    }

    public boolean createRole(String roleName) throws AuthException {
        return Authorizer.createRole(roleName);
    }

    public boolean addPermissionToRole(String roleName, String nodeName, int permissionId)
            throws AuthException {
        return Authorizer.addPmsToRole(roleName, nodeName, permissionId);
    }

    public boolean removePermissionFromRole(String roleName, String nodeName, int permissionId)
            throws AuthException {
        return Authorizer.removePmsFromRole(roleName, nodeName, permissionId);
    }


    public boolean deleteRole(String roleName) throws AuthException {
        return Authorizer.deleteRole(roleName);
    }


    public boolean grantRoleToUser(String roleName, String username) throws AuthException {
        return Authorizer.grantRoleToUser(roleName, username);
    }

    public boolean revokeRoleFromUser(String roleName, String username) throws AuthException {
        return Authorizer.revokeRoleFromUser(roleName, username);
    }

    public Set<Integer> getPermissionsOfUser(String username, String nodeName) throws AuthException {
        return Authorizer.getPermission(username, nodeName);
    }

    public PhysicalPlan queryPhysicalOptimize(PhysicalPlan plan) {
        return plan;
    }

    public PhysicalPlan nonQueryPhysicalOptimize(PhysicalPlan plan) {
        return plan;
    }

    public List<String> getAllPaths(String fullPath) throws PathErrorException {
        return getMManager().getPaths(fullPath);
    }
}
