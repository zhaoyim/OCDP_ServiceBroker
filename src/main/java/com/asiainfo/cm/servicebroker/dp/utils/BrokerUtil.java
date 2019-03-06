package com.asiainfo.cm.servicebroker.dp.utils;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.support.LdapNameBuilder;

import com.asiainfo.cm.servicebroker.dp.client.etcdClient;

import org.springframework.ldap.core.AttributesMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.ldap.LdapName;
import javax.naming.NamingException;
import java.util.List;
import java.util.UUID;
import java.io.IOException;

/**
 * Created by baikai on 10/17/16.
 */
public class BrokerUtil {

    private static final Logger logger = LoggerFactory.getLogger(BrokerUtil.class);

    private final static String uidNumberBase = "1500";

    public static void authentication(Configuration conf, String userPrincipal, String keyTabFilePath){
        UserGroupInformation.setConfiguration(conf);
        try{
            UserGroupInformation.loginUserFromKeytab(userPrincipal, keyTabFilePath);
        }catch (IOException e){
            logger.error("authentication() hit IOException: ", e);
        }
    }

    public static String generateAccountName(int digits){
        String uuid = UUID.randomUUID().toString();
        return uuid.substring(0,digits);
    }

    public static void createLDAPUser(LdapTemplate ldapTemplate, etcdClient etcdClient, String userName, String groupName, String gidNumber){
        String baseDN = "ou=People";
        LdapName ldapName = LdapNameBuilder.newInstance(baseDN)
                .add("uid", userName)
                .build();
        Attributes userAttributes = new BasicAttributes();
        userAttributes.put("memberOf", "cn=" + groupName +",ou=Group,dc=asiainfo,dc=com");
        BasicAttribute classAttribute = new BasicAttribute("objectClass");
        classAttribute.add("account");
        classAttribute.add("posixAccount");
        userAttributes.put(classAttribute);
        userAttributes.put("cn", userName);
        userAttributes.put("uidNumber", getNextUidNumber(etcdClient));
        userAttributes.put("gidNumber", gidNumber);
        userAttributes.put("homeDirectory", "/home/" + userName);
        ldapTemplate.bind(ldapName, null, userAttributes);
    }

    public static void removeLDAPUser(LdapTemplate ldapTemplate, String userName){
        String baseDN = "ou=People";
        LdapName ldapName = LdapNameBuilder.newInstance(baseDN)
                .add("uid", userName)
                .build();

        ldapTemplate.unbind(ldapName);
    }

    public static boolean isLDAPUserExist(LdapTemplate ldapTemplate, String userName){
        List list = ldapTemplate.search(
                "", "(uid=" + userName + ")",
                new AttributesMapper() {
                    public Object mapFromAttributes(Attributes attrs)
                            throws NamingException {
                        return attrs.get("uid").get();
                    }
                });
        return (list.size() != 0);
    }

    private static synchronized String getNextUidNumber(etcdClient etcdClient){
        String uidNumber = etcdClient.readToString("/servicebroker/ocdp/user/uidNumber");
        if(uidNumber == null){
            etcdClient.write("/servicebroker/ocdp/user/uidNumber", uidNumberBase);
            return uidNumberBase;
        }
        int uidNumberInt = Integer.parseInt(uidNumber);
        String nextUidNumber = Integer.toString(++uidNumberInt);
        etcdClient.write("/servicebroker/ocdp/user/uidNumber", nextUidNumber);
        return nextUidNumber;
    }

}
