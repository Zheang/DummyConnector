package icf.bundle;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeInfo;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.ObjectClassInfoBuilder;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.ResultsHandler;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.SchemaBuilder;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.PoolableConnector;
import org.identityconnectors.framework.spi.operations.CreateOp;
import org.identityconnectors.framework.spi.operations.DeleteOp;
import org.identityconnectors.framework.spi.operations.SchemaOp;
import org.identityconnectors.framework.spi.operations.SearchOp;
import org.identityconnectors.framework.spi.operations.UpdateOp;




public class DummyConnector implements PoolableConnector, CreateOp, DeleteOp, UpdateOp, SchemaOp{


	private DummyConfiguration config;
	private DatabaseCommon dbcommon;
	private Schema schema;
	
	
	@Override
	public void dispose() {
		// TODO Auto-generated method stub
		dbcommon.closeConnection();
		
	}

	@Override
	public Configuration getConfiguration() {
		// TODO Auto-generated method stub
		return config;
	}

	@Override
	public void init(Configuration cfg) {
        System.out.println("init DatabaseTable connector");
        if (cfg == null)
            throw new IllegalStateException("Configuration in init is null");

        if (!cfg.equals(this.config)) {
            this.config = (DummyConfiguration) cfg;
            this.schema = null;
            dbcommon = new DatabaseCommon((DummyConfiguration)cfg);
        }
        System.out.println("init DatabaseTable connector ok, connection is valid");
		
	}




	@Override
	public void checkAlive() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void delete(final ObjectClass oclass, final Uid uid, final OperationOptions options) {
        

        if (oclass == null || (!oclass.equals(ObjectClass.ACCOUNT))) {
            throw new IllegalArgumentException("ACCOUNT_OBJECTCLASS_REQUIRED");
        }
        

        if (uid == null || (uid.getUidValue() == null)) {
            throw new IllegalArgumentException("UID_IS_BLANK");
        }
        final String accountUid = uid.getUidValue();
        final String key = config.getKeyColumn();

        try {
            dbcommon.openConnection();
            doSchema();
            
            List<String> para = new ArrayList<String>();
            para.add(key);
            para.add(accountUid);
            String sql = dbcommon.sqlGenerator("delete", para);
            dbcommon.excute(sql);
            
                       
            dbcommon.commit();
        } catch (Exception e) {
            dbcommon.rollback();
            throw ConnectorException.wrap(e);
        } finally {
            dbcommon.closeConnection();
        }

       
    }

	@Override
	public Uid create(ObjectClass arg0, Set<Attribute> arg1,
			OperationOptions arg2) {
		// TODO Auto-generated method stub
		Uid uid = null;
        try {
            dbcommon.openConnection();
            doSchema();
           
            uid = AttributeUtil.getUidAttribute(arg1);
            List<String> para = new ArrayList<String>();
    		for(Attribute attr : arg1)
    		{
    			if(attr!= null) {
    				para.add(attr.getName());
    				para.add((String)attr.getValue().get(0));
    			}
    			
    		}
    		String sql = dbcommon.sqlGenerator("create", para);
    		dbcommon.excute(sql);
            dbcommon.commit();
        } catch (Exception e) {
            dbcommon.rollback();
            throw ConnectorException.wrap(e);
        } finally {
            dbcommon.closeConnection();
        }
        return uid;
		
		
	}

	@Override
	public Schema schema() {
		try {
            dbcommon.openConnection();                
            doSchema();
            dbcommon.commit();
        } catch (Exception e) {
            dbcommon.rollback();
            throw ConnectorException.wrap(e);
        } finally {
            dbcommon.closeConnection();
        }
        System.out.println("schema");
        return schema;
	}
	
	public void doSchema() throws SQLException{
		if(schema == null) {
			List<String> para = new ArrayList<String>();
			String query = dbcommon.sqlGenerator("select", para);
			
			ResultSet rset = dbcommon.excute(query);
			dbcommon.initTableInfo(rset);
			
			SchemaBuilder schemaBld = new SchemaBuilder(getClass());
			Set<AttributeInfo> attrInfos = dbcommon.getAttrInfos();
			
			schemaBld.defineObjectClass(ObjectClass.ACCOUNT.getDisplayNameKey(),
					attrInfos);
			
			schema = schemaBld.build();
		}
	}

	@Override
	public Uid update(ObjectClass oclass, Uid uid, Set<Attribute> attrs, OperationOptions options) {
        
        if (oclass == null || (!oclass.equals(ObjectClass.ACCOUNT))) {
            throw new IllegalArgumentException("Object class required!");
        }

        if (attrs == null || attrs.size() == 0) {
            throw new IllegalArgumentException("Invalid attribute set");
        }

        final String accountUid = uid.getUidValue();

        Uid ret = null;
        
        try {
            dbcommon.openConnection();
            doSchema();
            List<String> para = new ArrayList<String>();
            para.add(config.getKeyColumn());
            para.add(accountUid);
            for(Attribute attr : attrs){
            	para.add(attr.getName());
            	para.add((String)attr.getValue().get(0));
            }
            String sql = dbcommon.sqlGenerator("update", para);
            dbcommon.excute(sql);
            dbcommon.commit();
            ret = uid;
        } catch (Exception e) {
            dbcommon.rollback();
            throw ConnectorException.wrap(e);
        } finally {
            dbcommon.closeConnection();
        }
        return ret;
    }

}
