import paramiko
from membase.api.rest_client import RestConnection
import testconstants
from remote.remote_util import RemoteMachineShellConnection
from tuq import QueryTests


class AdvancedQueryTests(QueryTests):
    def setUp(self):
        super(AdvancedQueryTests, self).setUp()
        self.use_rest = False


    def tearDown(self):
        if self._testMethodName == 'suite_tearDown':
            self.skip_buckets_handle = False
        super(AdvancedQueryTests, self).tearDown()

    def test_engine_postive(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                try:
                    o = shell.execute_commands_inside('%s/cbq' % (self.path),'\quit','','','','','','')
                    print o
                    self.assertTrue("Exitingtheshell" in o)
                finally:
                    shell.disconnect()


    def test_shell_error(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                try:
                    o = shell.execute_commands_inside('%s/cbq  -q ' % (self.path),'\quit1','','','','','')
                    print o
                    self.assertTrue("FAIL" in o)
                finally:
                    shell.disconnect()


    def test_engine_ne(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                try:
                    o = shell.execute_commands_inside('%s/cbq  -q -ne' % (self.path),'select * from %s' % bucket.name,'','','','','')
                    print o
                    self.assertTrue('FAIL' in o)
                    o = shell.execute_commands_inside('%s/cbq -q -ne' % (self.path),'\SET','','','','','')
                    print o
                finally:
                    shell.disconnect()

    # rest parameter timeout, give with - option for rest parameters
    def test_timeout(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                try:
                    queries = ['\set -timeout "10ms";',"create primary index on bucketname;","select * from bucketname;"]
                    o = shell.execute_commands_inside('%s/cbq -q ' % (self.path),'',queries,bucket.name,'',bucket.name,'')
                    print o
                    self.assertEqual('timeout',o[7:])
                finally:
                    shell.disconnect()

    # difference combinations of username/password and creds
    def check_onesaslbucket_auth(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                try:
                    if (bucket.saslPassword != ''):
                        print('sasl')
                        # o = shell.execute_commands_inside('%s/cbq -q -u %s -p %s' % (self.path,bucket.name,bucket.saslPassword),'CREATE PRIMARY INDEX ON %s USING GSI' %bucket.name,'','','','','')
                        # self.assertTrue("requestID" in o)
                        # o = shell.execute_commands_inside('%s/cbq -q -u %s -p %s' % (self.path,bucket.name,bucket.saslPassword),'select *,join_day from %s limit 10'%bucket.name,'','','','','')
                        # self.assertTrue("requestID" in o)
                        # o = shell.execute_commands_inside('%s/cbq -q -u %s -p %s' % (self.path,bucket.name,'wrong'),'select * from %s limit 10'%bucket.name,'','','','','')
                        # self.assertTrue("AuthorizationFailed"  in o)
                        #
                        # o = shell.execute_commands_inside('%s/cbq -q -u %s -p %s' % (self.path,'','wrong'),'select * from %s limit 10'%bucket.name,'','','','','')
                        # self.assertEqual('FAIL',o[7:])
                        # o = shell.execute_commands_inside('%s/cbq -q -u %s -p %s' % (self.path,'wrong',bucket.saslPassword),'select * from %s limit 10'%bucket.name,'','','','','')
                        # self.assertTrue("AuthorizationFailed"  in o)
                        # queries = ['\set -creds user:pass;','select *,join_day from bucketname limit 10;']
                        # o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,bucket.name,bucket.saslPassword,bucket.name,'' )
                        # self.assertTrue("requestID" in o)
                        #
                        # queries = ['\set -creds user:pass;','select * from bucketname union all select * from default limit 10;']
                        # o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'Administrator','password',bucket.name,'' )
                        # self.assertTrue("requestID" in o)
                        queries = ['\set -creds user:pass;','SELECT buck.email FROM  bucketname buck LEFT JOIN default on keys "query-testemployee10153.1877827-0";']
                        o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'Administrator','password',bucket.name,'' )
                        self.assertTrue("requestID" in o)
                        queries = ['\set -creds user:pass;','SELECT buck.email FROM  bucketname buck LEFT JOIN default on keys "query-testemployee10153.1877827-0";']
                        o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,bucket.name,bucket.saslPassword,bucket.name,'' )
                        self.assertTrue("requestID" in o)

                        # queries = ['select count(*) from bucketname  union all select count(*) from default;']
                        # o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'',bucket.saslPassword,bucket.name,''  )
                        # self.assertTrue("AuthorizationFailed"  in o)
                        #
                        # queries = ['\set -creds user:pass;','select *,email,join_day from bucketname;']
                        # o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'Administrator','password',bucket.name,'' )
                        # self.assertTrue("requestID" in o)
                        queries = ['\set -creds user:pass;','select email,join_day from bucketname union all select email,join_day from default;']
                        o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,bucket.name,bucket.saslPassword,bucket.name,'' )
                        self.assertTrue("requestID" in o)
                        #
                        # o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'wrong','wrong',bucket.name,'' )
                        # self.assertTrue("AuthorizationFailed"  in o)
                        # o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'wrong',bucket.saslPassword,bucket.name,'' )
                        # self.assertTrue("AuthorizationFailed"  in o)
                        # o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,bucket.name,'wrong',bucket.name,'' )
                        # self.assertTrue("AuthorizationFailed"  in o)
                        # o = shell.execute_commands_inside('%s/cbq -q -u=%s -p=%s' % (self.path,'Administrator','password'),'select * from %s limit 10;' %bucket.name,'','','','','' )
                        # print o;
                        # self.assertTrue("requestID" in o)
                        # o = shell.execute_commands_inside('%s/cbq -q -u=%s -p=%s' % (self.path,bucket.name,bucket.saslPassword),'select * from %s limit 10;' %bucket.name,'','','','','' )
                        # self.assertTrue("requestID" in o)
                        print('nonsasl')
                        o = shell.execute_commands_inside('%s/cbq -q -u %s -p %s' % (self.path,'Administrator','password'),'select * from default limit 10;','','','','','' )
                        print o;
                        self.assertTrue("requestID" in o)
                        o = shell.execute_commands_inside('%s/cbq -q -u %s -p %s' % (self.path,bucket.name,bucket.saslPassword),'select * from default limit 10;' ,'','','','','' )
                        print o;
                        self.assertTrue("requestID" in o)
                        o = shell.execute_commands_inside('%s/cbq -q ' % (self.path),'select * from default limit 10;','','','','','' )
                        self.assertTrue("requestID" in o)

                finally:
                    shell.disconnect()


    def check_multiple_saslbuckets_auth(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            #import pdb;pdb.set_trace()
            queries = ['\set -creds bucket0:pass,bucket1:pass;','create primary index on bucket0;','create primary index on bucket1;','select count(*) from bucket0  union all select count(*) from bucket1;']
            o = shell.execute_commands_inside('%s/cbq --quiet' % (self.path),'',queries,'bucket1','password','bucket0','' )
            print o
            self.assertTrue("requestID" in o)
            queries = ['SELECT buck.email FROM  bucketname buck LEFT JOIN default on keys "query-testemployee10153.1877827-0";']
            o = shell.execute_commands_inside('%s/cbq --quiet' % (self.path),'',queries,'bucket1','password','bucket0','' )
            print o
            self.assertTrue("AuthorizationFailed" in o)
            queries = ['\set -creds bucket0:pass,bucket1:pass;','SELECT buck.email FROM  bucketname buck LEFT JOIN default on keys "query-testemployee10153.1877827-0 limit 10";']
            o = shell.execute_commands_inside('%s/cbq --quiet' % (self.path),'',queries,'bucket0','password','bucket1','' )
            print o
            self.assertTrue("requestID" in o)

            queries = ['\set -creds Administrator:pass;','select * from bucket0 union all select * from bucket1 limit 10;']
            o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'bucket0','password','bucket1','' )
            self.assertTrue("requestID" in o)

            queries = ['\set -creds username:pass;','SELECT buck.email FROM  bucket0 buck LEFT JOIN bucket1 on keys "query-testemployee10153.1877827-0";']
            o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'bucket0','password123','bucket1',''  )
            print o
            self.assertTrue("AuthorizationFailed" in o)


            queries = ['\set -creds Administrator:pass;','select * from bucketname union all select * from default limit 10;']
            o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'bucket1','password','bucket0','' )
            self.assertTrue("requestID" in o)

            queries = ['\set -creds user:pass;','select * from bucketname union all select * from default limit 10;']
            o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'bucket0','password','bucket2','' )
            self.assertTrue("requestID" in o)

            #import pdb;pdb.set_trace()
            queries = ['\set -creds wrong:pass1,user:pass;','drop primary index on bucket1;','drop primary index on bucket2;']
            o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'bucket0' ,'password','bucket1','' )
            print o
            queries = ['\set -creds user1:pass1,'':pass2;','create primary index on bucket1;','create primary index on bucket2;']
            o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'bucket0','password','bucket1','')
            print o
            queries = ['\set -creds '':pass1,'':pass2;','drop primary index on bucket1;','drop primary index on bucket2;']
            o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'bucket0','password','bucket1','' )
            print o


    def test_version(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            o = shell.execute_commands_inside('%s/cbq --version' % (self.path),'','','','','','' )
            print o
            o = shell.execute_commands_inside('%s/cbq -s="\HELP VERSION"' % (self.path),'','','','','','' )
            print o

    #
    # def test_invalid_input_url(self):
    #
    def test_connect_disconnect(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                queries = ['\connect http://localhost:8091;','create primary index on bucketname;']
                o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'','',bucket.name,'' )
                print o
                queries = ['\connect http://localhost:8091;','drop primary index on bucketname;']
                o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'','',bucket.name,'' )
                print o
                # wrong disconnect
                queries = ['\disconnect http://localhost:8091;','create primary index on bucketname;']
                o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'','',bucket.name,'' )
                self.assertTrue("Toomanyinputargumentstocommand" in o)
                print o
                #wrong port
                queries = ['\connect http://localhost:8097;','create primary index on bucketname;']
                o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'','',bucket.name,'' )
                self.assertTrue("Unabletoconnectto" in o)
                print o
                #wrong url including http
                queries = ['\connect http://localhost345:8097;','create primary index on bucketname;']
                o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'','',bucket.name,'' )
                print o
                self.assertTrue("Unabletoconnectto" in o)
                #wrong url not including http
                queries = ['\connect localhost3458097;','create primary index on bucketname;']
                o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'','',bucket.name,'' )
                print o
                self.assertTrue("Unabletoconnectto" in o)
                queries = ['\disconnect','drop primary index on bucketname;']
                o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'','',bucket.name,'' )
                print o
                self.assertTrue("Toomanyinputargumentstocommand" in o)
                queries = ['\disconnect','create primary index on bucketname;']
                o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'','',bucket.name,'' )
                self.assertTrue("Toomanyinputargumentstocommand" in o)
                print o
                queries = ['\connect http://localhost:8091;','create primary index on bucketname;']
                o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'','',bucket.name,'' )
                print o

    def test_history(self):
         for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                if self.path == testconstants.WIN_COUCHBASE_BIN_PATH:
                    queries = ["\set histfile /cygdrive/c/tmp/history.txt;"]
                elif self.path == testconstants.LINUX_COUCHBASE_BIN_PATH:
                    queries = ["\set histfile /tmp/history;"]
                queries.extend(["\ALIAS tempcommand create primary index on bucketname;""\\\\tempcommand;",'\ALIAS tempcommand2 select * from bucketname limit 10;',"\\\\tempcommand2;",'\ALIAS;','\echo tempcommand1;','\echo tempcommand2;'])
                o = shell.execute_commands_inside('%s/cbq -quiet' % (testconstants.LINUX_COUCHBASE_BIN_PATH),'',queries,'','',bucket.name,'' )
                print o
                #open and check the file


    def test_alias_and_echo(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                queries = ["\ALIAS tempcommand create primary index on bucketname;","\\\\tempcommand;",'\ALIAS tempcommand2 select *,email from bucketname limit 10;',"\\\\tempcommand2;",'\ALIAS;','\echo tempcommand1;','\echo tempcommand2;']
                o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'','',bucket.name,'' )
                print o
                queries = ['\ALIAS tempcommand drop primary index on bucketname;','\\\\tempcommand;','\ALIAS tempcommand create primary index on bucketname;','\ALIAS tempcommand2 drop primary index on bucketname;','\\\\tempcommand;','\\\\tempcommand2;','\ALIAS;','\echo tempcommand;','\echo tempcommand2;']
                o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'','',bucket.name,'' )
                print o
                queries = ['\UNALIAS tempcommand drop primary index on bucketname;','\\\\tempcommand;']
                o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'','',bucket.name,'' )
                self.assertTrue("Aliasdoesnotexist" in o)
                print o
                queries = ['\UNALIAS tempcommand;','\\\\tempcommand;']
                o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'','',bucket.name,'' )
                self.assertTrue("Aliasdoesnotexist" in o)
                print o




    def test_positional_params(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                queries = ['\SET -args [7, 0,1,2011];','prepare temp from SELECT tasks_points.task1 AS task from bucketname WHERE join_mo>$1 GROUP BY tasks_points.task1 HAVING COUNT(tasks_points.task1) > $2 AND  (MIN(join_day)=$3 OR MAX(join_yr=$4)) ORDER BY tasks_points.task1 ;','execute temp;']
                o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'','',bucket.name ,'')
                print o



    def test_named_params(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                queries = ['\SET -$join_day 2;','\SET -$project "AB";','prepare temp from select name, tasks_ids,join_day from bucketname where join_day>=$join_day and tasks_ids[0] IN (select ARRAY_AGG(DISTINCT task_name) as names from bucketname d use keys ["test_task-1", "test_task-2"] where project!=$project)[0].names;','execute temp;']
                o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'','',bucket.name,'' )
                print o

    def test_push_pop_set(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                i=1
                pushqueries =[]
                while(i<15):
                    pushqueries.append('\SET -args [7, 0,1,2011];')
                    pushqueries.append('\push;')
                    pushqueries.append('\SET -$join_day %s;' %i)
                    pushqueries.append('\push -$join_day %s;' %i)
                    pushqueries.append('\push -args [8,1,2,2011];')
                    pushqueries.append('select $join_day;');
                    pushqueries.append('\SET -$project "AB";')
                    pushqueries.append('\push;')
                    pushqueries.append('\push  -$project "CD";')
                    pushqueries.append('select  $project;')
                    pushqueries.append('prepare temp from select  tasks_points.task1 AS task from bucketname where join_day>=$join_day and  join_mo>$1 GROUP BY tasks_points.task1 HAVING COUNT(tasks_points.task1) > $2 AND  (MIN(join_day)=$3 OR MAX(join_yr=$4));')
                    pushqueries.append('execute temp;')
                    pushqueries.append('\set;')
                    i=i+1
                    o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',pushqueries,'','',bucket.name,'' )
                    print o
                i=1
                popqueries =[]
                while(i<10):
                    popqueries.append('\SET;')
                    popqueries.append('\pop;')
                    popqueries.append('\pop -args;')
                    popqueries.append('\pop -$join_day;')
                    popqueries.append('select $join_day;');
                    popqueries.append('\pop -$project;')
                    popqueries.append('\SET;')
                    popqueries.append('prepare temp from select tasks_points.task1 AS task from bucketname where join_day>=$join_day and  join_mo>$1 GROUP BY tasks_points.task1 HAVING COUNT(tasks_points.task1) > $2 AND  (MIN(join_day)=$3 OR MAX(join_yr=$4));')
                    popqueries.append('execute temp;')
                    i=i+1
                    o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',popqueries,'','',bucket.name,'' )
                    print o

    def test_redirect(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for bucket in self.buckets:
                queries = ['\\redirect abc;','create primary index on bucketname;','select name,tasks_points.task1,skills from bucketname;','\\redirect off;']
                o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'','',bucket.name,'' )
                sftp = shell._ssh_client.open_sftp()
                fileout = sftp.open("abc",'r')
                filedata = fileout.read()
                print filedata
                queries = ['drop primary index on bucketname;']
                o = shell.execute_commands_inside('%s/cbq -quiet' % (self.path),'',queries,'','',bucket.name,True )
                print o


#select * from,select multiple columns, select *,multiple columns from--same results
                    #add node,rebalance,rem
    # def test_file_input_and_source




