#!/usr/bin/env python
#-*- coding=utf8 -*-

import os
import shutil
import time
import logging
import mysql.connector
import tarfile


### init directory
backup_path = "/tmp/backup" 
dirname = time.strftime('%Y-%m-%d_%H%M%S',time.localtime(time.time()))
backup_dirname =  "%s/%s" % (backup_path , dirname)  
xtrabackup_dir = "%s/xtrabackup" % backup_dirname
mysqldump_dir = "%s/mysqldump" % backup_dirname

if os.path.exists("%s" % backup_dirname):
    pass
else:
    print 'create backup directory: %s' % backup_dirname
    os.makedirs('%s' % backup_dirname) 


### define log and wrapper
backup_step_log = '%s/backup_step.log' % backup_path
logging.basicConfig(filename=backup_step_log,
                    level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s:%(filename)s: %(message)s',
                    filemode='w'
                    ) 

def use_logging(func):
  def wrapper(*args, **kwargs):
      logging.info("%s start" % func.__name__)
      if func.__name__ == "_xtrabackup" or func.__name__ == "_apply_log":
         func(*args, **kwargs)
         filename = '%s/%s.log' % (backup_dirname,func.__name__)
         with open(filename, 'r') as f:
             lines = f.readlines() 
             last_line = lines[-1]
             logging.info("%s" % last_line)
      elif func.__name__ == "_mysqldump" or func.__name__ == "_compress":
          func(*args, **kwargs)
          logging.info("%s end" % func.__name__)
      else:
          pass
  return wrapper
### connect mysql info
db_config = {
        'host':'127.0.0.1',
        'port':'3306',
        'user':'root',
        'password':'fffjjj',
        } 

### xtrabackup backup
@use_logging
def _xtrabackup():
    if os.path.exists('%s' % xtrabackup_dir):
        pass
    else:
        print 'create backup directory: %s' % xtrabackup_dir
        os.makedirs('%s' % xtrabackup_dir)  

    cmd = "innobackupex  --host={host} --port={port} --user={user} --password={password} --slave-info --no-timestamp %s 2>%s/_xtrabackup.log".format(**db_config)  % (xtrabackup_dir ,backup_dirname)
    os.system(cmd)
### xtrabackup apply log
@use_logging 
def _apply_log():
    os.chdir("%s" % backup_path)
    cmd = "innobackupex --apply-log %s 2>%s/_apply_log.log" % (xtrabackup_dir ,backup_dirname)
    os.system(cmd)

### mysqldump
@use_logging 
def _mysqldump():
    #create directory
    if os.path.exists('%s' % mysqldump_dir):
        pass
    else:
        print 'create backup directory: %s' % mysqldump_dir
        os.makedirs('%s' % mysqldump_dir) 

    #get mysqldump database list
    db_list = []
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()
    query = ("select SCHEMA_NAME from information_schema.SCHEMATA where SCHEMA_NAME not in('test','binlog_','dbhealth','information_schema','performance_schema')")
    cursor.execute(query)
    results = cursor.fetchall()
    result=list(results)
    
    for r in result:
        db_list.append(('%s' % r))
    cursor.close()
    cnx.close() 
    
    #backup command
    for db in db_list:
        cmd = "/srv/mysql3306/bin/mysqldump --host={host} --port={port} --user={user} --password={password} --single-transaction --master-data=2 %s > %s/%s.sql".format(**db_config) % (db,mysqldump_dir,db)
        os.system(cmd)
### partition manager
def _partition_manager():
    import time,datetime
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()
    taday = time.strftime("%Y-%m-%d", time.localtime()) + " 00:00:00" 

    #获得配置表分区信息
    query_config_table_info = (""" select * from mulberry_test.mb_table_config order by table_id; """)
    cursor.execute(query_config_table_info)
    query_config_table_info_results = cursor.fetchall()
    for r in query_config_table_info_results:
        Schema_name = r[1]
        Table_name = r[2]
        #获得服器分区表信息
        #query_server_partition_info = (""" select table_schema,table_name,partition_name,partition_description from information_schema.partitions where partition_name is not null; """)  
        query_server_partition_info = (""" select table_schema,table_name from information_schema.partitions where partition_name is not null group by table_schema,table_name; """)  
        cursor.execute(query_server_partition_info)
        query_server_partition_info_results = cursor.fetchall()
        #如果服务器中的库名，表名与配置表中的库名，表名一致则进入清理逻辑
        for r in query_server_partition_info_results:           
            if r[0]==Schema_name and r[1]==Table_name:
               # 获得配置表需要分的分区信息
               query_config_table_need_drop_info = (""" select * from mulberry_test.mb_table_config where schema_name="%s" and table_name="%s" order by table_id; """) %(Schema_name,Table_name)
               cursor.execute(query_config_table_need_drop_info)
               query_config_table_need_drop_info_results = cursor.fetchall()
               for r in query_config_table_need_drop_info_results:
                   schema_name = r[1]
                   table_name = r[2]
                   forward_day = r[3]
                   clear_before_day = r[4] 
                   #print "%s.%s" %(schema_name,table_name)  #需要处理的库名，表名

                   # 获得指库表需要清理的分区
                   query_server_clear_before_day_partition = ("""select partition_name from information_schema.partitions
                                                where partition_name is not null and table_schema='%s' and table_name='%s' and partition_description != "maxvalue"
                                                and partition_description<= unix_timestamp(date_sub("%s",interval %s day))*1000
                                                order by table_schema,table_name,partition_ordinal_position asc;""") % (schema_name,table_name,taday,clear_before_day)
                   cursor.execute(query_server_clear_before_day_partition)
                   query_server_clear_before_day_partition_results = cursor.fetchall()
                   # drop clear before day partition
                   for partition in query_server_clear_before_day_partition_results:
                       if partition:
                           partition_name = partition[0]
                           query_drop_partition_sql = ("alter table %s.%s drop partition %s;") % (schema_name,table_name,partition_name)
                           print query_drop_partition_sql
                           #cursor.execute(query_drop_partition_name)
                       else:
                           pass
                   # drop max partition :if max partition is null then drop max partition else not drop max partition
                   query_max_partition_sql = ("""select partition_name from information_schema.partitions 
                                         where table_schema='%s' and table_name='%s' and partition_description = "maxvalue";""") %(schema_name,table_name)
                   cursor.execute(query_max_partition_sql)
                   query_max_partition_sql_results = cursor.fetchall()
                   for partition in query_max_partition_sql_results:
                       if partition:
                           max_partition_name = partition[0]
                           query_not_empty = ("""select 1 from %s.%s partition(%s) limit 1;""") % (schema_name,table_name,max_partition_name)
                           cursor.execute(query_not_empty)
                           query_not_empty_results = cursor.fetchall()
                           if query_not_empty_results:
                               print "The %s.%s partition %s is not empty" %(schema_name,table_name,max_partition_name)
                           else:
                               query_drop_max_partition_sql = ("""alter table %s.%s drop partition %s; """) % (schema_name,table_name,max_partition_name)
                               print query_drop_max_partition_sql 
                               #cursor.execute(query_drop_max_partition)
                       else:
                           pass
                   # get exist forward day partition
                  # query_server_forward_day_partition = ("""select partition_description from information_schema.partitions
                  #                              where partition_name is not null and table_schema='%s' and table_name='%s' and partition_description != "maxvalue"
                  #                              and partition_description> unix_timestamp(date_sub("%s",interval -%s day))*1000
                  #                              order by table_schema,table_name,partition_ordinal_position asc;""") % (schema_name,table_name,taday,forward_day) 
                   query_server_forward_day_partition_sql = ("""select partition_description from information_schema.partitions
                                                where partition_name is not null and table_schema='%s' and table_name='%s' and partition_description != "maxvalue"
                                                order by table_schema,table_name,partition_ordinal_position asc;""") % (schema_name,table_name) 
                   cursor.execute(query_server_forward_day_partition_sql)
                   query_server_forward_day_partition_sql_results = cursor.fetchall() 
                   print query_server_forward_day_partition_sql_results
                   for i in range(forward_day):
                       now = datetime.datetime.now()
                       s=now.strftime('%Y-%m-%d')
                       d = datetime.datetime.strptime(s,"%Y-%m-%d")
                       partition_description =  int(time.mktime(d.timetuple()) + 3600*24*i)*1000
                       suffix_time = time.strftime("%Y%m%d",time.localtime((partition_description -3600*24)/1000))
                       add_patition_name = "p_auto_new_%s" % suffix_time
#                       if len(query_server_forward_day_partition_results) == 0:
#                           pass
#                       else:
#                           for description in query_server_forward_day_partition_results:
#                                print description
#                               if description[0] == partition_description:
#                                   print description
#                               else:
#                                   print description[0]
#                              # print "i=%s schema_name=%s" %(i,schema_name)
#                              # if len(query_server_forward_day_partition_results) != 0:
#                              #     query_add_partiiton = (""" alter table %s.%s add partition(partition %s values less than(%s));""") % (schema_name,table_name,add_patition_name,partition_description)
#                              #     print query_add_partiiton
#                           #else:
#                           #    for description in query_server_forward_day_partition_results:
#                           #        if description:
#                           #            print description[0] + 'aa'
#                           #            query_add_partiiton = (""" alter table %s.%s add partition(partition %s values less than("%s"))""") % (schema_name,table_name,add_patition_name,partition_description)
#                           #        else:
#                           #            query_add_partiiton = (""" alter table %s.%s add partition(partition %s values less than("%s"))""") % (schema_name,table_name,add_patition_name,partition_description)
#                           #            print query_add_partiiton
#                           #            pass
#                               
    cursor.close()
    cnx.close()

### 压缩备份 
@use_logging
def _compress():
    os.chdir("%s" % backup_path)
    target_tar = '%s/%s.tar.gz' %(backup_path,dirname) 

    cmd = "tar -zcf %s %s" % (target_tar,dirname)
    os.system(cmd)
### 删除当天备份目录、超出保留天数的备份
def _rmove_backup():
    os.chdir("%s" % backup_path)
    target_tar = '%s/%s.tar.gz' %(backup_path,dirname)
    if os.path.isfile("%s" % target_tar):
        file_size = (os.path.getsize('%s' % target_tar)/1024/1024)
        logging.info("The %s size: %s MB" % (target_tar,file_size))
        shutil.rmtree("%s" % dirname)
    logging.info("The disk info:")
    os.system("df -hT /data >>%s" % backup_step_log)
    
### 邮件发送
def _send_mail():
    import smtplib
    from email.mime.text import MIMEText      #导入MIMEText类
    from email.header import Header 

    smtp_server = 'smtp.exmail.qq.com'
    from_mail = 'dbserver@ibeesaas.com'
    mail_pass = 'Ibeesaas2017'
    to_mail = ['gaochao@ibeesaas.com', ]
    cc_mail = ['gaochao@ibeesaas.com']
    msg_pre = """ """                                #邮件正文
    with open(r'%s' % backup_step_log) as f:
        while True:
            line = f.readline()
            msg_pre += line.strip()+'\n'
            if not line:
                break
    msg = MIMEText('%s' % msg_pre,'plain','utf-8')  
    subject = 'Backup infomation'                    #邮件标题
    msg['Subject'] = Header(subject,'utf-8')
    msg['From'] = Header('%s' % from_mail,'utf-8')   #显示发件人信息
    msg['To'] = Header('%s' % to_mail,'utf-8')       #显示收件人信息
    msg['Cc'] = Header('%s' % cc_mail,'utf-8')       #显示抄送人信息
    msg_pre = """ """                                #邮件正文  
    try:
        s = smtplib.SMTP()
        s.connect(smtp_server, '25')
        s.login(from_mail, mail_pass)
        s.sendmail(from_mail, to_mail + cc_mail, msg.as_string())
        s.quit()
    except smtplib.SMTPException as e:
        print "Error: %s" %e 

if __name__ == "__main__":
    #_xtrabackup()
    #_apply_log()
    #_mysqldump()
    #_compress()
    #_rmove_backup()
    #_send_mail()
    _partition_manager()




