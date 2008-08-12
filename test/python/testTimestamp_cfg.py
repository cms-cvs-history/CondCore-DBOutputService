import FWCore.ParameterSet.Config as cms

process = cms.Process("TEST")
process.source = cms.Source("EmptyIOVSource",
    lastTime = cms.untracked.uint32(15),
    timetype = cms.string('timestamp'),
    firstTime = cms.untracked.uint32(11),
    interval = cms.uint32(1)
)

process.PoolDBOutputService = cms.Service("PoolDBOutputService",
    DBParameters = cms.PSet(
        messageLevel = cms.untracked.int32(0),
        authenticationPath = cms.untracked.string('.')
    ),
    timetype = cms.untracked.string('timestamp'),
    connect = cms.string('sqlite_file:test.db'),
    toPut = cms.VPSet(cms.PSet(
        record = cms.string('PedestalsRcd'),
        tag = cms.string('mytest')
    ))
)

process.mytest = cms.EDAnalyzer("Timestamp",
    record = cms.string('PedestalsRcd')
)

process.p = cms.Path(process.mytest)


