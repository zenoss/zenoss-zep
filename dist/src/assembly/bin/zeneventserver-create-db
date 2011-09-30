#!/usr/bin/env python

import optparse
import os
import subprocess
import sys

class SchemaMigration(object):
    def __init__(self, dbname, dbhost=None, dbport=None, dbrootuser=None, dbrootpass=None, dbuser=None, dbpass=None,
                 **args):
        self.dbname = dbname
        self.dbhost = dbhost
        self.dbport = dbport
        self.dbrootuser = dbrootuser
        self.dbrootpass = dbrootpass
        self.dbuser = dbuser
        self.dbpass = dbpass
        for k, v in args.iteritems():
            self.__dict__[k] = v

    def dropDatabase(self):
        """
        Drops the self.dbname database (if it exists).
        """
        raise NotImplementedError()

    def createDatabase(self):
        """
        Creates the self.dbname database (if it doesn't exist).
        """
        raise NotImplementedError()

    def getSchemaVersion(self):
        """
        Returns the current version of the zeneventserver database schema, or 0 if the database is empty.
        """
        raise NotImplementedError()

    def applySchemaFile(self, filename):
        """
        Applies the schema in the specified filename (full path).
        """
        raise NotImplementedError()

class MysqlMigration(SchemaMigration):
    def _getAdminCommand(self):
        cmd = ['mysql', '--batch', '--skip-column-names']
        if self.dbhost:
            cmd.extend(['--host', self.dbhost])
        if self.dbport:
            cmd.extend(['--port', str(self.dbport)])
        if self.dbrootuser:
            cmd.extend(['--user', self.dbrootuser])
        if self.dbrootpass:
            cmd.extend(['--password=%s' % self.dbrootpass])
        return cmd

    def dropDatabase(self):
        cmd = self._getAdminCommand()
        cmd.extend(['-e', 'DROP DATABASE IF EXISTS %s' % self.dbname])
        subprocess.check_call(cmd)

    def createDatabase(self):
        cmd = self._getAdminCommand()
        cmd_in = "CREATE DATABASE IF NOT EXISTS {0.dbname};\n".format(self)
        if self.dbhost and self.dbuser and self.dbpass:
            cmd_in += """
GRANT ALL ON {0.dbname}.* TO '{0.dbuser}'@'{0.dbhost}' IDENTIFIED BY '{0.dbpass}';
GRANT ALL ON {0.dbname}.* TO '{0.dbuser}'@'%' IDENTIFIED BY '{0.dbpass}';
FLUSH PRIVILEGES;""".format(self)

        p = subprocess.Popen(cmd, stdin=subprocess.PIPE)
        p.communicate(cmd_in)
        rc = p.wait()
        if rc:
            raise subprocess.CalledProcessError(rc, cmd)

    def getSchemaVersion(self):
        cmd = self._getAdminCommand()
        cmd.extend(['-e', 'SELECT MAX(version) FROM %s.schema_version' % self.dbname])
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        cmd_out, cmd_err = p.communicate()
        p.wait()
        version = cmd_out.strip()
        return int(version) if version else 0

    def applySchemaFile(self, filename):
        cmd = self._getAdminCommand()
        cmd.extend([self.dbname])
        cmd_in = "SOURCE {0};".format(filename)

        p = subprocess.Popen(cmd, stdin=subprocess.PIPE)
        p.communicate(cmd_in)
        rc = p.wait()
        if rc:
            raise subprocess.CalledProcessError(rc, cmd)

class PostgresMigration(SchemaMigration):
    def _passwordFile(self):
        from tempfile import NamedTemporaryFile
        tf = NamedTemporaryFile()
        tf.write('*:*:*:*:{0.dbrootpass}\n'.format(self))
        tf.flush()
        return tf

    def _getAdminCommand(self, command):
        cmd = [command]
        cmd.extend(['-h', self.dbhost])
        if self.dbport:
            cmd.extend(['-p', str(self.dbport)])
        cmd.extend(['-U', self.dbrootuser])
        cmd.extend(['-w'])
        return cmd

    def _executeCmd(self, cmd, save_streams=False):
        with self._passwordFile() as pf:
            env = os.environ.copy()
            env['PGPASSFILE'] = pf.name
            stdout, stderr = None, None
            if save_streams:
                p = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                stdout, stderr = p.communicate()
            else:
                p = subprocess.Popen(cmd, env=env)
            return p.wait(), stdout, stderr

    def _listDatabases(self):
        # List databases on the system
        listCmd = self._getAdminCommand('psql')
        listCmd.extend(['-A', '-t', '--list'])
        rc, stdout, stderr = self._executeCmd(listCmd, save_streams=True)
        if rc:
            raise subprocess.CalledProcessError(cmd=listCmd, output=stdout, returncode=rc)

        databases = []
        for line in stdout.splitlines():
            fields = line.split('|')
            if fields:
                databases.append(fields[0])
        return databases

    def dropDatabase(self):
        # Don't attempt to drop a database that doesn't exist
        if not self.dbname in self._listDatabases():
            return
        
        dropCmd = self._getAdminCommand('dropdb')
        dropCmd.append(self.dbname)
        rc, stdout, stderr = self._executeCmd(dropCmd)
        if rc:
            raise subprocess.CalledProcessError(cmd=dropCmd, returncode=rc)

    def createDatabase(self):
        # Don't recreate the database if it already exists
        if self.dbname in self._listDatabases():
            return

        createCmd = self._getAdminCommand('createdb')
        createCmd.extend(['-O', self.dbuser])
        createCmd.append(self.dbname)
        rc, stdout, stderr = self._executeCmd(createCmd)
        if rc:
            raise subprocess.CalledProcessError(cmd=createCmd, returncode=rc)

    def getSchemaVersion(self):
        psqlCmd = self._getAdminCommand('psql')
        psqlCmd.extend(['-t', '-A'])
        psqlCmd.extend(['-d', self.dbname])
        psqlCmd.extend(['-c', 'SELECT MAX(version) FROM schema_version'])
        rc, stdout, stderr = self._executeCmd(psqlCmd, save_streams=True)
        return int(stdout) if stdout else 0

    def applySchemaFile(self, filename):
        psqlCmd = self._getAdminCommand('psql')
        psqlCmd.append('-q')
        psqlCmd.extend(['-d', self.dbname])
        psqlCmd.extend(['-f', filename])
        # Save output because psql is really chatty
        rc, stdout, stderr = self._executeCmd(psqlCmd, save_streams=True)
        if rc:
            raise subprocess.CalledProcessError(cmd=psqlCmd, returncode=rc, output=stdout)

def list_schema_files(directory):
    if not os.path.isdir(directory):
        raise ValueError('Invalid schema directory: %s' % directory)
    import re
    schema_files = []
    schema_file_pattern = re.compile(r'^(\d+)\.sql$')
    for entry in os.listdir(directory):
        full_path = os.path.join(directory, entry)
        if os.path.isfile(full_path):
            match = re.match(schema_file_pattern, entry)
            if match:
                schema_files.append((int(match.group(1)), full_path))
    return schema_files

if __name__ == '__main__':
    zenhome = os.getenv('ZENHOME', '/opt/zenoss')
    parser = optparse.OptionParser()
    # TODO: Change this to postgres
    parser.add_option('--dbtype', dest='dbtype', default='mysql', help='Database Type')
    parser.add_option('--dbname', dest='dbname', default='zenoss_zep', help='Database Name')
    parser.add_option('--dbhost', dest='dbhost', default='localhost', help='Database Host')
    parser.add_option('--dbport', type='int', dest='dbport', help='Database Port')
    parser.add_option('--dbrootuser', dest='dbrootuser', default='root', help='Database Admin User')
    parser.add_option('--dbrootpass', dest='dbrootpass', default='', help='Database Admin Password')
    parser.add_option('--dbuser', dest='dbuser', default='zenoss', help='Database User')
    parser.add_option('--dbpass', dest='dbpass', default='zenoss', help='Database Password')
    parser.add_option('--schemadir', dest='schemadir', default=os.path.join(zenhome, 'share', 'zeneventserver', 'sql'))
    parser.add_option('--force', dest='force', action='store_true', help='Overwrite existing database')
    options, args = parser.parse_args()

    migrator_classes = {
        'mysql': MysqlMigration,
        'postgresql': PostgresMigration,
    }
    if options.dbtype not in migrator_classes:
        parser.error('Unknown database type: %s' % options.dbtype)
    if options.dbport is not None and (options.dbport < 1 or options.dbport > 65535):
        parser.error('Invalid port: %d' % options.dbport)

    schema_files = list_schema_files(os.path.join(options.schemadir, options.dbtype))
    migrator = migrator_classes[options.dbtype](**vars(options))
    try:
        if options.force:
            print 'Dropping database: %s' % options.dbname
            migrator.dropDatabase()
        print 'Creating database: %s' % options.dbname
        migrator.createDatabase()
        current_version = migrator.getSchemaVersion()
        for schema_version, schema_file in schema_files:
            if schema_version > current_version:
                print 'Applying schema version: %d' % schema_version
                migrator.applySchemaFile(schema_file)
    except subprocess.CalledProcessError as e:
        sys.exit(e.returncode)

    