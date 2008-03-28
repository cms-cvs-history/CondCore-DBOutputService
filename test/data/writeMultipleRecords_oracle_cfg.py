import FWCore.ParameterSet.Config as cms

process = cms.Process("TEST")
process.load("CondCore.DBCommon.CondDBCommon_cfi")

process.source = cms.Source("EmptyIOVSource",
    lastRun = cms.untracked.uint32(1),
    timetype = cms.string('runnumber'),
    firstRun = cms.untracked.uint32(1),
    interval = cms.uint32(1)
)

process.PoolDBOutputService = cms.Service("PoolDBOutputService",
    process.CondDBCommon,
    BlobStreamerName = cms.untracked.string('TBufferBlobStreamingService'),
    timetype = cms.untracked.string('runnumber'),
    toPut = cms.VPSet(cms.PSet(
        record = cms.string('PedestalsRcd'),
        tag = cms.string('ped_tag')
    ), cms.PSet(
        record = cms.string('mySiStripNoisesRcd'),
        tag = cms.string('noise_tag')
    ))
)

process.mytest = cms.EDFilter("writeMultipleRecords")

process.p = cms.Path(process.mytest)
process.CondDBCommon.connect = 'oracle://cms_orcoff_int2r/CMS_COND_PRESH'
process.CondDBCommon.DBParameters.authenticationPath = '/afs/cern.ch/cms/DB/conddb'


