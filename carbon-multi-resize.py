#!/usr/bin/env python
#
# Run whisper-resize on all *.wsp files under top directory to set their
# storage size and aggregations to match current values configured in
# storage-schemas.conf and storage-aggregations.conf
#
# Usage:
# export GRAPHITE_ROOT=....                       # or
# export GRAPHITE_STORAGE_DIR=/var/lib/graphite   # Debian or Ubuntu
# carbon-multi-resize [UPDATE_BASE]
#
# where UPDATE_BASE is a subdirectory of GRAPHITE_STORAGE_DIR, eg
# "/var/lib/graphite/whisper/collected/device1".  If omitted, all of
# GRAPHITE_STORAGE_DIR will be processed.
#
# *.wsp files that already have storage space matching what is
# configured in storage-schemas.conf will be automatically skipped.
#
# Environment variables recognised, to match Debian /etc/carbon
# settings:
# - GRAPHITE_ROOT
# - GRAPHITE_CONF_DIR
# - GRAPHITE_STORAGE_DIR
# - GRAPHITE_BIN_DIR
#
# The program will attempt to automatically locate unspecified values,
# either relative to GRAPHITE_ROOT, or in Debian/Ubuntu packaged default
# locations if those exist.
#
# Copyright viralshah (https://github.com/viralshah), 2013.
# Copyright TheVirtual Ltd http://www.thevirtual.co.nz/), 2015.
#
# Original written by viralshah (https://github.com/viralshah), 2013
# Updated by Ewen McNeill <ewen@naos.co.nz>, 2015-06-30, for Debian layout
#---------------------------------------------------------------------------
#
import os
from os.path import abspath, dirname, exists, join, normpath
import re
import subprocess
import sys

from carbon.conf import OrderedConfigParser
from carbon.util import pickle
import carbon.exceptions
import whisper

# Permit path overrides from environment
#   GRAPHITE_ROOT        - Root directory of the graphite installation.
#                          Defaults to ../
#   GRAPHITE_CONF_DIR    - Configuration directory (where this file lives).
#                          Defaults to $GRAPHITE_ROOT/conf/
#   GRAPHITE_STORAGE_DIR - Storage directory for whipser/rrd/log/pid files.
#                          Defaults to $GRAPHITE_ROOT/storage/
#   GRAPHITE_BIN_DIR     - Directory with whisper-resize
#                          Defaults to $GRAPHITE_ROOT/bin/
#
GRAPHITE_ROOT        = os.environ.get('GRAPHITE_ROOT')
GRAPHITE_STORAGE_DIR = os.environ.get('GRAPHITE_STORAGE_DIR')
GRAPHITE_CONF_DIR    = os.environ.get('GRAPHITE_CONF_DIR')
GRAPHITE_BIN_DIR     = os.environ.get('GRAPHITE_BIN_DIR')

# Base for updates, from command line (optional, defaults to LOCAL_DATA_DIR
# below if not set)
UPDATE_BASE          = (sys.argv + [None])[1]

# Defaults, relative to GRAPHITE_DIR
if GRAPHITE_ROOT and not GRAPHITE_STORAGE_DIR:
    GRAPHITE_STORAGE_DIR = join(GRAPHITE_ROOT, 'storage')

if GRAPHITE_ROOT and not GRAPHITE_CONF_DIR:
    GRAPHITE_CONF_DIR = join(GRAPHITE_ROOT, 'conf')

if GRAPHITE_ROOT and not GRAPHITE_BIN_DIR:
    GRAPHITE_BIN_DIR  = join(GRAPHITE_ROOT, 'bin')

# Debian specific overrides, used if the graphite default ones are missing
# and these ones exist (ie, looks like a Debian/Ubuntu package layout)
DEBIAN_GRAPHITE_BIN_DIR     = '/usr/bin'
DEBIAN_GRAPHITE_CONF_DIR    = '/etc/carbon'
DEBIAN_GRAPHITE_STORAGE_DIR = '/var/lib/graphite'
DEBIAN_WHISPER_RESIZE       = 'whisper-resize'

if exists(DEBIAN_GRAPHITE_BIN_DIR) and (not GRAPHITE_BIN_DIR or
                                 not exists(GRAPHITE_BIN_DIR)):
    GRAPHITE_BIN_DIR = DEBIAN_GRAPHITE_BIN_DIR

if exists(DEBIAN_GRAPHITE_CONF_DIR) and (not GRAPHITE_CONF_DIR or
                                  not exists(GRAPHITE_CONF_DIR)):
    GRAPHITE_CONF_DIR = DEBIAN_GRAPHITE_CONF_DIR

if exists(DEBIAN_GRAPHITE_STORAGE_DIR) and (not GRAPHITE_STORAGE_DIR or
                                     not exists(GRAPHITE_STORAGE_DIR)):
    GRAPHITE_STORAGE_DIR = DEBIAN_GRAPHITE_STORAGE_DIR

# Validate that we actually have paths to everything
if not (GRAPHITE_STORAGE_DIR and GRAPHITE_CONF_DIR and GRAPHITE_BIN_DIR):
    print("Unable to find graphite directories - set one or more of:")
    print("    GRAPHITE_ROOT")
    print("    GRAPHITE_BIN_DIR")
    print("    GRAPHITE_CONF_DIR")
    print("    GRAPHITE_STORAGE_DIR")
    print("(defaults are relative to GRAPHITE_ROOT)")
    sys.exit(1)

# Derived paths to specific things
WHITELISTS_DIR       = join(GRAPHITE_STORAGE_DIR, 'lists')
LOCAL_DATA_DIR       = join(GRAPHITE_STORAGE_DIR, 'whisper')

WHISPER_RESIZE       = join(GRAPHITE_BIN_DIR, 'whisper-resize.py')
if exists(join(GRAPHITE_BIN_DIR, DEBIAN_WHISPER_RESIZE)) and \
                             not exists(WHISPER_RESIZE):
    WHISPER_RESIZE   = join(GRAPHITE_BIN_DIR, DEBIAN_WHISPER_RESIZE)

STORAGE_SCHEMAS_CONFIG = join(GRAPHITE_CONF_DIR, 'storage-schemas.conf')
STORAGE_AGGREGATION_CONFIG = join(GRAPHITE_CONF_DIR, 'storage-aggregation.conf')

# Default to processing all local data, but otherwise ensure that
# path names are likely to appear to be within LOCAL_DATA_DIR
if UPDATE_BASE:
    UPDATE_BASE = os.path.abspath(UPDATE_BASE)
else:
    UPDATE_BASE = os.path.abspath(LOCAL_DATA_DIR)

# Hacky test that UPDATE_BASE is inside LOCAL_DATA_DIR
if not normpath(LOCAL_DATA_DIR) in UPDATE_BASE:
    print("UPDATE_BASE must be a subdirectory of LOCAL_DATA_DIR")
    print("(otherwise storage schemas selection will fail, and all")
    print(" files will end up with default storage schema)")
    sys.exit(1)

#---------------------------------------------------------------------------

class Schema:
  def test(self, metric):
    raise NotImplementedError()

  def matches(self, metric):
    return bool( self.test(metric) )


class DefaultSchema(Schema):

  def __init__(self, name, archives):
    self.name = name
    self.archives = archives

  def test(self, metric):
    return True


class PatternSchema(Schema):

  def __init__(self, name, pattern, archives):
    self.name = name
    self.pattern = pattern
    self.regex = re.compile(pattern)
    self.archives = archives

  def test(self, metric):
    return self.regex.search(metric)

class ListSchema(Schema):

  def __init__(self, name, listName, archives):
    self.name = name
    self.listName = listName
    self.archives = archives
    self.path = join(WHITELISTS_DIR, listName)

    if exists(self.path):
      self.mtime = os.stat(self.path).st_mtime
      fh = open(self.path, 'rb')
      self.members = pickle.load(fh)
      fh.close()

    else:
      self.mtime = 0
      self.members = frozenset()

  def test(self, metric):
    if exists(self.path):
      current_mtime = os.stat(self.path).st_mtime

      if current_mtime > self.mtime:
        self.mtime = current_mtime
        fh = open(self.path, 'rb')
        self.members = pickle.load(fh)
        fh.close()

    return metric in self.members


class Archive:

  def __init__(self,secondsPerPoint,points):
    self.secondsPerPoint = int(secondsPerPoint)
    self.points = int(points)

  def __str__(self):
    return "Archive = (Seconds per point: %d, Datapoints to save: %d)" % (self.secondsPerPoint, self.points)

  def getTuple(self):
    return (self.secondsPerPoint,self.points)

  @staticmethod
  def fromString(retentionDef):
    (secondsPerPoint, points) = whisper.parseRetentionDef(retentionDef)
    return Archive(secondsPerPoint, points)


def loadStorageSchemas():
  schemaList = []
  config = OrderedConfigParser()
  config.read(STORAGE_SCHEMAS_CONFIG)

  for section in config.sections():
    options = dict( config.items(section) )
    matchAll = options.get('match-all')
    pattern = options.get('pattern')

    retentions = options['retentions'].split(',')
    archives = [ Archive.fromString(s) for s in retentions ]

    if matchAll:
      mySchema = DefaultSchema(section, archives)

    elif pattern:
      mySchema = PatternSchema(section, pattern, archives)

    archiveList = [a.getTuple() for a in archives]

    try:
      whisper.validateArchiveList(archiveList)
      schemaList.append(mySchema)
    except whisper.InvalidConfiguration, e:
      print "Invalid schemas found in %s: %s" % (section, e)

  schemaList.append(defaultSchema)
  return schemaList


def loadAggregationSchemas():
  # NOTE: This abuses the Schema classes above, and should probably be refactored.
  schemaList = []
  config = OrderedConfigParser()

  try:
    config.read(STORAGE_AGGREGATION_CONFIG)
  except IOError:
    print "%s not found, ignoring." % STORAGE_AGGREGATION_CONFIG
  except carbon.exceptions.CarbonConfigException:
    print "%s not found, ignoring." % STORAGE_AGGREGATION_CONFIG

  for section in config.sections():
    options = dict( config.items(section) )
    matchAll = options.get('match-all')
    pattern = options.get('pattern')

    xFilesFactor = options.get('xfilesfactor')
    aggregationMethod = options.get('aggregationmethod')

    try:
      if xFilesFactor is not None:
        xFilesFactor = float(xFilesFactor)
        assert 0 <= xFilesFactor <= 1
      if aggregationMethod is not None:
        assert aggregationMethod in whisper.aggregationMethods
    except:
      print "Invalid schemas found in %s." % section
      continue

    archives = (xFilesFactor, aggregationMethod)

    if matchAll:
      mySchema = DefaultSchema(section, archives)

    elif pattern:
      mySchema = PatternSchema(section, pattern, archives)

    schemaList.append(mySchema)

  schemaList.append(defaultAggregation)
  return schemaList

defaultArchive = Archive(60, 60 * 24 * 7) #default retention for unclassified data (7 days of minutely data)
defaultSchema = DefaultSchema('default', [defaultArchive])
defaultAggregation = DefaultSchema('default', (None, None))


print "Loading storage-schemas configuration from: '%s'" % STORAGE_SCHEMAS_CONFIG
schemas = loadStorageSchemas()

print "Loading storage-aggregation configuration from: '%s'" % STORAGE_AGGREGATION_CONFIG
agg_schemas = loadAggregationSchemas()

#print schemas
#print agg_schemas

def get_archive_config(metric):
    archiveConfig = None
    xFilesFactor, aggregationMethod = None, None

    for schema in schemas:
      if schema.matches(metric):
        #print 'new metric %s matched schema %s' % (metric, schema.name)
        archiveConfig = [archive.getTuple() for archive in schema.archives]
        break

    for schema in agg_schemas:
      if schema.matches(metric):
        #print 'new metric %s matched aggregation schema %s' % (metric, schema.name)
        xFilesFactor, aggregationMethod = schema.archives
        break

    if not archiveConfig:
        raise Exception("No storage schema matched the metric '%s', check your storage-schemas.conf file." % metric)

    return (archiveConfig, xFilesFactor, aggregationMethod)

def diff_file_conf(metric, filepath):
    """
    Returns true if the actual file has parameters different from those in the configuration files
    """
    (archiveConfig, xFilesFactor, aggregationMethod) = get_archive_config(metric)

    info = whisper.info(filepath)

    if info['xFilesFactor'] != xFilesFactor or info['aggregationMethod'] != aggregationMethod:
        #print "{0} {1}".format(info['aggregationMethod'], aggregationMethod)
        #print "{0} {1}".format(info['xFilesFactor'], xFilesFactor)
        return True

    for (archivefile, archiveconf) in zip(info['archives'], archiveConfig):
        (secondsPerPoint, points) = archiveconf
        #print "{0} {1}".format(archivefile['secondsPerPoint'], secondsPerPoint)
        #print "{0} {1}".format(archivefile['points'], points)
        if archivefile['secondsPerPoint'] != secondsPerPoint or archivefile['points'] != points:
            return True

print("Processing data in %s" % UPDATE_BASE)
wsp_regex = re.compile('\.wsp$')
root_dir_regex = re.compile('^' + LOCAL_DATA_DIR + os.sep)
dir_sep_regex = re.compile(os.sep)

for root, dirs, files in os.walk(UPDATE_BASE):
    for filename in [f for f in files if wsp_regex.search(f)]:
        filepath = join(root, filename)
        metric = dir_sep_regex.sub('.', wsp_regex.sub('',
                                   root_dir_regex.sub('', filepath)))
        print "Processing {0}".format(filepath)
        if diff_file_conf(metric, filepath):
            #there is a difference and we need to resize the whisper file
            (archiveConfig, xFilesFactor, aggregationMethod) = get_archive_config(metric)
            command_args = [WHISPER_RESIZE, filepath]
            for (secondsPerPoint, points) in archiveConfig:
                command_args.append("{0}:{1}".format(secondsPerPoint, points))

            command_args.append('--nobackup')

            if aggregationMethod:
                command_args.append('--aggregationMethod={0}'.format(aggregationMethod))

            if xFilesFactor is not None:
                command_args.append('--xFilesFactor={0}'.format(xFilesFactor))

            #print ' '.join(command_args)
            subprocess.check_output(command_args)
