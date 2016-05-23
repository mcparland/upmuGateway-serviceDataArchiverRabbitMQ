/*
 * Copyright (C) 2016 Chuck McParland All rights reserved.
 */

/* 
 * File:   serviceInstanceTemplateApp2.cpp
 * Author: mcp
 * 
 * Created on January 24, 2016, 5:01 PM
 */
#include <iostream>
#include <fstream>
#include <chrono>
#include <time.h>
#include <thread>
#include <atomic>
#include <set>
#include <pqxx/pqxx>
#include "cassandra.h"
#include <boost/make_shared.hpp>
#include <boost/thread.hpp>
#include <boost/ref.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/dll/alias.hpp>
#include <boost/thread/condition.hpp>
#include <boost/any.hpp>
#include <log4cpp/Appender.hh>
#include <log4cpp/Category.hh>
#include <log4cpp/FileAppender.hh>
#include <log4cpp/OstreamAppender.hh>
#include <log4cpp/Layout.hh>
#include <log4cpp/BasicLayout.hh>
#include <log4cpp/Priority.hh>
#include <SimpleAmqpClient/SimpleAmqpClient.h>
#include <amqp.h>

#include "dbConnection.h"
#include "serviceCommand.h"
#include "serviceDataBuffer.h"

#include "serviceCommandQueueTemplate.h"
#include "serviceInstanceApi.h"
#include "serviceStatus.h"
#include "upmuDataProtobuf.pb.h"
#include "serviceOperationsLoggerPostgres.h"
#include "serviceOperationsLoggerCassandra.h"
#include "upmuStructKeyValueListProtobuf.pb.h"

using namespace serviceCommon;

#include "serviceCommonStats.h"
#include "serviceSpecificStats.h"
#include "uPMUparticulars.h"
#include "uPMUgmtime.h"

#include "serviceDataArchiverRabbitMQ.h"

namespace serviceDataArchiverRabbitMQ {
using namespace boost;
using namespace std;
using namespace serviceCommon;
using namespace std::chrono;
using namespace AmqpClient;

serviceDataArchiverRabbitMQ::serviceDataArchiverRabbitMQ() {
    std::cout<< "Creating serviceDataArchiverRabbitMQ" << std::endl;
    cmdQueue = new SynchronisedCommandQueue<SERVICE_COMMAND *>(CMD_QUEUE_LEN);
    m_thread=NULL;
    m_mustStop=false;
    serviceStatus = NOT_STARTED;
}

serviceDataArchiverRabbitMQ::~serviceDataArchiverRabbitMQ() {
    std::cout<< "serviceDataArchiverRabbitMQ::~serviceDataArchiverRabbitMQ" << std::endl;
    if (m_thread!=NULL) delete m_thread;
}

// Start the thread 
void serviceDataArchiverRabbitMQ::start() {
    //std::cout<< "serviceDataArchiverRabbitMQ::start" << std::endl;
    // Pass myself as reference since I don't want a copy
    m_mustStop = false;  
    /* start thread.  operator() () is the function executed */
    m_thread=new boost::thread(boost::ref(*this));
}
 
// Stop the thread
void serviceDataArchiverRabbitMQ::stop() {
    //std::cout<< "serviceDataArchiverRabbitMQ::stop" << std::endl;
    // Signal the thread to stop (thread-safe)
    serviceStatus = STOPPING;
    m_mustStopMutex.lock();
    m_mustStop=true;
    m_mustStopMutex.unlock();
    // notify running thread
    wake_up.notify_one();
 
    // Wait for the thread to finish.
    if (m_thread!=NULL) m_thread->join();
}
 
dataBufferOffer_t serviceDataArchiverRabbitMQ::dataBufferOffer(
    const std::shared_ptr<serviceDataBuffer>& offeredBuf) {    
    std::cout<< "serviceDataArchiverRabbitMQ::dataBufferOffer" << std::endl;
   
    serviceCmnStatsMutex.lock();
    unsigned int * cnt = any_cast<unsigned int *>
        ((*serviceCmnStatsValues)[INDX_UINT_NUM_DATA_BUF_SEEN]);
    *cnt++;
    serviceCmnStatsMutex.unlock();
    
    switch (DATA_QUEUE_LEN - dataQueue.size()) {
        case 0: {
            /* no room for buffer,  return rejected */
            serviceCmnStatsMutex.lock();
            unsigned int * cnt = any_cast<unsigned int *>
                ((*serviceCmnStatsValues)[INDX_UINT_NUM_DATA_BUF_REJECTED]);
            *cnt++;
            serviceCmnStatsMutex.unlock();
            return BUFFER_REFUSED;
        }
        default: {
            /* buffer accepted, ownership transferred */
            m_dataQueueMutex.lock();  
            std::shared_ptr<serviceDataBuffer> ptr = offeredBuf;
            dataQueue.push(ptr);
            m_dataQueueMutex.unlock();
            
            serviceCmnStatsMutex.lock();
            unsigned int * cnt = any_cast<unsigned int *>
                ((*serviceCmnStatsValues)[INDX_UINT_NUM_DATA_BUF_ACCEPTED]);
            *cnt++;
            serviceCmnStatsMutex.unlock();
            return BUFFER_ACCEPTED;
        }
    }
    return BUFFER_ACCEPTED;
}

// Thread function
void serviceDataArchiverRabbitMQ::operator () () {
    //std::cout<< "serviceDataArchiverRabbitMQ::operator" << std::endl;
    
    bool localMustStop = false;
    SERVICE_COMMAND * cmd;
    serviceStatus = RUNNING;
    
    /* create log4cpp related appender objects for this service. */
    logger_serviceDataArchiverRabbitMQ.addAppender(logger_appender);
    /* log startup message */
    logger_serviceDataArchiverRabbitMQ.info("serviceDataArchiver starting.");
    
    if(!initService()) {
        /* log error and exit*/
        logger_serviceDataArchiverRabbitMQ.error("Could not complete initialization, exiting.");
        return;
    }
    
    int testCnt = 0;
    try {
        /* buffer processing context record */
        std::shared_ptr<serviceDataBuffer> dataBuf = nullptr;
        bool processingBuffer = false;
        
        while(!localMustStop) {
            testCnt++;
            /* always check for possible command */
            cmd = cmdQueue->Dequeue();
            if(cmd != 0) {
                processCmd(cmd);
            }
            
            /* are we processing a buffer */
            if(processingBuffer) {
                if(dataBuf == nullptr) {
                    /* Error TBD */
                }
                else {
                    bool complete = processData(&bpr, dataBuf);
                    if(complete) {
                        /* process insert timer */
                        if(bpr.numInsertsInAccum > 0) {
                            float avgInsertTime = bpr.accumInsertTime /
                            bpr.numInsertsInAccum;
                            /* insert into stats */
                            serviceSpecStatsMutex.lock();
                            float * minInsertTime = any_cast<float *>
                                ((*serviceSpecStatsValues)[INDX_FLT_MIN_AVG_DATA_INSERT_MSEC]);
                            float * maxInsertTime = any_cast<float *>
                                ((*serviceSpecStatsValues)[INDX_FLT_MAX_AVG_DATA_INSERT_MSEC]);
                            if(*minInsertTime == 0.0) {
                                *minInsertTime = avgInsertTime;
                            }
                            else if(avgInsertTime < *minInsertTime) {
                                *minInsertTime = avgInsertTime;
                            }
                            if(*maxInsertTime == 0.0) {
                                *maxInsertTime = avgInsertTime;
                            }
                            else if(avgInsertTime > *maxInsertTime) {
                                *maxInsertTime = avgInsertTime;
                            }
                            float * ptrAvgInsertTime = any_cast<float *>
                                ((*serviceSpecStatsValues)[INDX_FLT_LAST_AVG_DATA_INSERT_MSEC]);
                            *ptrAvgInsertTime = avgInsertTime;
                            serviceSpecStatsMutex.unlock();
                            
                        }
                        /* release the buffer and move to next */
                        dataBuf.reset();
                        processingBuffer = false;
                    }
                    else {
                        /* set up to handle next record in buffer */
                       
                    }
                }
            }
            /* check to see if any databuffers are queued */
            else if(!dataQueue.empty()) {
                /* get msg buffer from non-empty queue */
                m_dataQueueMutex.lock();
                dataBuf = dataQueue.front();
                dataQueue.pop();
                m_dataQueueMutex.unlock();
                /* preprocess msg buffer to check for simple format errors and
                 compute tho many 1 sec. buffers are present. */
                int sts = preProcessData(&bpr, dataBuf);
                if(sts <= 0) {
                    /* buffer error */
                    serviceSpecStatsMutex.lock();
                    unsigned int * cnt = any_cast<unsigned int *>
                        ((*serviceSpecStatsValues)[INDX_UINT_ABANDONED_BUFFERS]);
                    *cnt++;
                    serviceSpecStatsMutex.unlock();
                    /* release the buffer by reseting the shared pointer   If no
                     one else is using this buffer, it gets deallocated by its
                     destructor */
                    dataBuf.reset();
                }
                else {
                    /* we're processing this buffer. loop over 1 sec. buffers 
                     contained until everything is processed. Processing starts
                     on the next pass through this loop*/
                    processingBuffer = true;
                }
            }
            /* if we are not procesing a buffer, sleep for a bit and then
            any new requests */
             if(!processingBuffer && dataQueue.empty()) {
                 std::this_thread::sleep_for(std::chrono::seconds(SCAN_MODE_NORMAL_DELAY_SEC));
                 
             }
            /* Get the "must stop" state (thread-safe) for inspection at the
             * top of this loop */
            m_mustStopMutex.lock();
            localMustStop=m_mustStop;
            m_mustStopMutex.unlock();
        }
        /* We got a stop request, so exiting function kills thread and
         * stops the service */
        serviceStatus = NORMAL_STOP;
        std::cout<< "Normal stop serviceDataQA::operator" << std::endl;
        return;
    }
    catch (const std::exception &e) {
        serviceStatus = ERROR_STOP;
        std::cout<< "Error stop serviceDataQA::operator" << std::endl;
        return;
    }
};

void serviceDataArchiverRabbitMQ::processCmd(SERVICE_COMMAND * cmd) {
    std::cout<< "serviceDataArchiverRabbitMQ::processCmd" << std::endl;
    
}

bool serviceDataArchiverRabbitMQ::initService() {
    serviceState = "normal";
    
    /* init service data structures */
    createServiceCommonStats(this);
    createServiceSpecificStats(this);
    
    /* get a db connection for local database */
    dbConnection * dbConn = dbConnection::getInstance();
    if(dbConn->conn == NULL) {
        /* log an error and exit. */
        logger_serviceDataArchiverRabbitMQ.error("Cannot get valid postgres connector object.");
        return false;
    }
    
    /* get connection info from database and establish conection with
     rabbitMQ server. */
    try {
        dbConn->acquireDBaccess();
        nontransaction nt(*(dbConn->conn));
        result r = nt.exec("SELECT * FROM rabbitmq ORDER BY last_modified DESC LIMIT 1");
        /* should be exactly one row */
        if(r.size() != 1) {
            logger_postgres.error("serviceDtaArchiverRabbitMQ: Multiple or missing rabbitmq config "
                    " records in postgres table: rabbitmq.");       
            return -1;
        }
        /* get important fields into local variables */
        pqxx::result::field field = r[0]["\"routekey\""];
        rabbitmqRoutekey = field.c_str();
        field = r[0]["\"channel\""];
        rabbitmqChannel = field.c_str();
        field = r[0]["\"vhost\""];
        rabbitmqVhost = field.c_str();
        field = r[0]["\"db_user\""];
        rabbitmqUser = field.c_str();
        field = r[0]["\"passwd\""];
        rabbitmqPasswd = field.c_str();
        field = r[0]["\"ip_address\""];
        rabbitmqIpAddress = field.c_str();       
        field = r[0]["\"port\""];
        rabbitmqPort = field.as<int>();
        field = r[0]["\"archiving_enabled\""];
        rabbitmqArchivingEnabled = field.as<bool>();
        
        nt.commit();
        dbConn->releaseDBaccess();
        
        if(rabbitmqArchivingEnabled) {
            /* set rabbitMQ properties flags */
            rabbitmqProp._flags = 0;
            /* Setup and connect to cluster */
            try {
                rabbitmqWriteChannel = Channel::Create(rabbitmqIpAddress.c_str(),
                    rabbitmqPort, rabbitmqUser.c_str(), rabbitmqPasswd.c_str(),
                    rabbitmqVhost.c_str(), RABBITMQ_MAX_FRAME);
                logger_rabbitmq.info("serviceDataArchiverRabbitMQ: rabbitMQ "
                    "channel object created.");
                rabbitmqConnected = true;
            }
            catch (std::exception& e) {
                logger_rabbitmq.error("serviceDataArchiverRabbitMQ: could not "
                    " create rabbitMQ channel object.");
                rabbitmqConnected = false;
            }
        }
        else {
            rabbitmqConnected = false;
            logger_rabbitmq.warn("serviceDataArchiverRabbitMQ: rabbitMq access disabled in "
                    "postgres table: rabbitmq.");
        }
        
    }
    catch (const std::exception &e) {
        /* log problem */
        std::string s = "serviceDataArchiverRabbitMQ: exception while "
            "creating rabbitMQ channel object: ";
        s.append(e.what());
        logger_rabbitmq.error(s.c_str());
        rabbitmqConnected = false;  
    }
    
    /* declare uPMUgateway started at this point. Note: startup message always 
     contains sessionID and major/minor version info.  Also, these params are ad hoc
     in the gateway; but are part of standard plugin statistices package in plugins. */
    postgresOperationsLogger->createStringOperationsRecord(dbConn, serviceState, upmuSerialNumber,
        sessionID, serviceName, "Service_Startup_Msg", "Initialized, waiting for input buffers.");
    postgresOperationsLogger->createStringOperationsRecord(dbConn, serviceState, upmuSerialNumber,
        sessionID, serviceName, 
        (*serviceSpecStatsLabels)[INDX_STR_DATA_ARCHIVER_RABBITMQ_MAJ_VER]->c_str(),
        any_cast<std::string *>((*serviceSpecStatsValues)[INDX_STR_DATA_ARCHIVER_RABBITMQ_MAJ_VER])->c_str());
    postgresOperationsLogger->createStringOperationsRecord(dbConn, serviceState, upmuSerialNumber,
        sessionID, serviceName, 
        (*serviceSpecStatsLabels)[INDX_STR_DATA_ARCHIVER_RABBITMQ_MIN_VER]->c_str(), 
        any_cast<std::string *>((*serviceSpecStatsValues)[INDX_STR_DATA_ARCHIVER_RABBITMQ_MIN_VER])->c_str());
    
    if(cassArchivingEnabled && cassConnected) {
        /* make a KeyValueList to log similar message to cassandra, if available.*/
        upmuStructKeyValueList * kvList = new upmuStructKeyValueList();
        createServiceStartupKeyValueList(kvList);
        uint32_t byteLen = kvList->ByteSize();
        void * pbBuffer = std::malloc(byteLen);
        if(pbBuffer != nullptr) {
            kvList->SerializeToArray(pbBuffer, byteLen);
            cassandraOperationsLogger->cassandraArchive(cassPreparedOperations, cassSession,
                (uint64_t)kvList->timestamp(), sessionID, serviceName,
                upmuSerialNumber, 33, 11, pbBuffer, byteLen);
            std::free(pbBuffer);
        } 
        else {
            logger_serviceDataArchiverRabbitMQ.error("Cannot form serialized protocol buffer, "
                "insufficient memory."); 
        }
    }
   
    /* init local values */
    dataPtr = nullptr;
    
    
    /* check  if cassandra arciving is possible and enabled */
    if(!rabbitmqArchivingEnabled || !rabbitmqConnected) {
        /* log abandoned buffer */
        logger_serviceDataArchiverRabbitMQ.warn("Cassandra access disabled or"
                " not available.  Proceeding anyway."); 
    }
    return true;
}

void serviceDataArchiverRabbitMQ::createServiceStartupKeyValueList(upmuStructKeyValueList * kvList) {
    kvList->set_name("Service_Startup_Info");
    uint32_t startTime = (uint32_t)time(NULL);
    kvList->set_timestamp(startTime);
    KeyValueList * list = kvList->add_list();
    list->set_category("ServiceStatus");

    /* add a few key values */
    serviceSpecStatsMutex.lock();
    KeyValue * pair = list->add_element();
    pair->set_key((*serviceSpecStatsLabels)[INDX_STR_DATA_ARCHIVER_RABBITMQ_MAJ_VER]->c_str()); 
    pair->set_stringval(any_cast<std::string *>
                ((*serviceSpecStatsValues)[INDX_STR_DATA_ARCHIVER_RABBITMQ_MAJ_VER])->c_str());

    pair = list->add_element();
    pair->set_key((*serviceSpecStatsLabels)[INDX_STR_DATA_ARCHIVER_RABBITMQ_MIN_VER]->c_str()); 
    pair->set_stringval(any_cast<std::string *>
                ((*serviceSpecStatsValues)[INDX_STR_DATA_ARCHIVER_RABBITMQ_MIN_VER])->c_str());
    serviceSpecStatsMutex.unlock();

}

int serviceDataArchiverRabbitMQ::preProcessData(
    BUFFER_PROCESSING_RECORD * bpr,
    std::shared_ptr<serviceDataBuffer>& dataBuf) {

    std::cout<< "serviceDataArchiverRabbitMQ::preProcessData" << std::endl;
    
    /* perform a few checks on data buffer to stay out of trouble */
    if((dataBuf->byteLen) % UPMU_1SEC_BUF_LEN != 0) {
        /* buffer does not have integral number of events, too risky to
         process */
        serviceSpecStatsMutex.lock();
        unsigned int * cnt = any_cast<unsigned int *>
            ((*serviceSpecStatsValues)[INDX_UINT_BUF_FMT_ERROR]);
        *cnt++;
        serviceSpecStatsMutex.unlock();
        bpr->numRecords = -1;
        return -1;
    }
    else if(!cassArchivingEnabled || !cassConnected) {
        /* Cassandra is disabled or not connected */
        serviceSpecStatsMutex.lock();
        unsigned int * cnt = any_cast<unsigned int *>
            ((*serviceSpecStatsValues)[INDX_UINT_INSERT_ABANDON_NO_RABBITMQ_CONN]);
        *cnt++;
        serviceSpecStatsMutex.unlock();
        bpr->numRecords = -1;
        return -1;
    }
    else {
        bpr->numRecords = dataBuf->byteLen / UPMU_1SEC_BUF_LEN;
        bpr->nextRecord = 0;
        bpr->numInsertsInAccum = 0;
        bpr->accumInsertTime = 0.0;
       
        return 1;
    }
}

bool serviceDataArchiverRabbitMQ::processData(BUFFER_PROCESSING_RECORD *bpr,
    std::shared_ptr<serviceDataBuffer>& dataBuf) {
    /* have we processed all the 1 sec. records discovered by the
     * preProcessData() routine?  If so, we're done with this buffer
     * and can release it and wait for next buffer to show up in the queue */
    if(bpr->nextRecord >= bpr->numRecords) {
        return true;
    }
    else {
        /* not done yet! */
        sync_output * secPtr = (sync_output *)(dataBuf->dataBuffer
            + bpr->nextRecord * UPMU_1SEC_BUF_LEN);
        /* create a new protobuf object to contain this 1 sec of data */
        upmuData secData;
        /* get an epoch time from uPMU discrete time struct and compute
         both days since epoch and epoch in msec. for later use */
        struct tm t = {0};  // Initalize to all 0's
        t.tm_year = secPtr->sectionTime.year - 1900;  // This is year-1900
        t.tm_mon = secPtr->sectionTime.month - 1;  // jan == 0
        t.tm_mday = secPtr->sectionTime.day;
        t.tm_hour = secPtr->sectionTime.hour;
        t.tm_min = secPtr->sectionTime.min;
        t.tm_sec = secPtr->sectionTime.sec;
        t.tm_zone = "GMT";
        time_t epochTime = _mkgmtime(&t);
        uint64_t epochTimeMsec = epochTime * 1000;
        uint64_t epochDay = epochTime / 86400;

        secData.set_timestamp(epochTime);
        secData.set_sampleintervalmsec(secPtr->sampleRate);
        secData.set_numsamples(UPMU_SAMPLES_PER_SEC);

        /* fill protobuf message w/contents of upmu sync_output struct */
        for(int indx = 0; indx < UPMU_SAMPLES_PER_SEC; indx++) {
            syncOutput * so = secData.add_sample();
            so->set_lockstate(secPtr->lockstate[indx]);

            so->set_l1angle(secPtr->L1MagAng->angle);
            so->set_l1mag(secPtr->L1MagAng->mag);

            so->set_l2angle(secPtr->L2MagAng->angle);
            so->set_l2mag(secPtr->L2MagAng->mag);

            so->set_l3angle(secPtr->L3MagAng->angle);
            so->set_l3mag(secPtr->L3MagAng->mag);

            so->set_c1angle(secPtr->C1MagAng->angle);
            so->set_c1mag(secPtr->C1MagAng->mag);

            so->set_c2angle(secPtr->C2MagAng->angle);
            so->set_c2mag(secPtr->C2MagAng->mag);

            so->set_c3angle(secPtr->C3MagAng->angle);
            so->set_c3mag(secPtr->C3MagAng->mag);  
        }
        
        /* check  if cassandra arciving is possible and enabled */
        if(!cassArchivingEnabled || !cassConnected) {
            /* log abandoned buffer */
            serviceSpecStatsMutex.lock();
            unsigned int * cnt = any_cast<unsigned int *>
                ((*serviceSpecStatsValues)[INDX_UINT_INSERT_ABANDON_NO_RABBITMQ_CONN]);
            *cnt++;
            serviceSpecStatsMutex.unlock();
            return false;
        }
        else {
            /* serialize the protobuf message by getting its length, allocating
             * sufficient memory and serializing it to the allocated memory. */
            unsigned int byteLen = secData.ByteSize();
            void * pbBuffer = std::malloc(byteLen);
            if(pbBuffer == nullptr) {
                /* we're out of memory log it.*/
                /* skip the buff.  Hope to catch error at queued message
                buffer level */
                serviceSpecStatsMutex.lock();
                unsigned int * cnt = any_cast<unsigned int *>
                    ((*serviceSpecStatsValues)[INDX_UINT_PB_NO_SERIAL_MEM_AVAIL]);
                *cnt++;
                serviceSpecStatsMutex.unlock();
                bpr->nextRecord++;
                return false;
            }       
            secData.SerializeToArray(pbBuffer, byteLen);

            /* archive it */
            unsigned int domainType = 3;
            unsigned int msgType = 1;
            /* create a rabbitmq basic message */
            rabbitmqPkt.bytes = pbBuffer;
            rabbitmqPkt.len = byteLen;
            rabbitmqMsg = BasicMessage::Create(rabbitmqPkt, &rabbitmqProp);
            /* send message */
            rabbitmqDataArchive(bpr);
            /* record is archived, so point to next second of data */
            bpr->nextRecord++;
            return false;
        }
        return false;
    }
}

int serviceDataArchiverRabbitMQ::rabbitmqDataArchive(BUFFER_PROCESSING_RECORD * bpr) {       
    const auto begin = high_resolution_clock::now(); // or use steady_clock if high_resolution_clock::is_steady is false
      
    try {
        rabbitmqWriteChannel->BasicPublish(rabbitmqRoutekey.c_str(),
            rabbitmqChannel.c_str(), rabbitmqMsg, false, false);
    }
    catch(std::exception& e) {
        /* BasicPubish frees buffer.  If we had a fault, it may
         not have been freed.  So, lets try it safely */
        
        serviceSpecStatsMutex.lock();
        unsigned int * cnt = any_cast<unsigned int *>
            ((*serviceSpecStatsValues)[INDX_UINT_RABBITMQ_DATA_INSERT_ERR]);
        *cnt++;
        serviceSpecStatsMutex.unlock();
        
        std::string s("Error publishing data protocol"
            " buffer to rabbitMQ server. Exception: ");
        /* count failures */
        s.append(e.what());
        logger_serviceDataArchiverRabbitMQ.error(s.c_str());
        try {
            std::free(rabbitmqPkt.bytes);
        }
        catch (std::exception& e) {
            /* nothing to do, but we'll log it. */
            std::string s("Error freeing protocol buffer within fault. "
                "Exception: ");
            s.append(e.what());
            logger_serviceDataArchiverRabbitMQ.error(s.c_str());
        }
    }
    
    auto time = high_resolution_clock::now() - begin;
    bpr->accumInsertTime += duration<float, std::milli>(time).count();
    bpr->numInsertsInAccum;
    //std::cout << "Elapsed time: " << duration<double, std::milli>(time).count() << ".\n";
    
}

int serviceDataArchiverRabbitMQ::cassandraCommonArchive(const CassPrepared * prepared,
        uint32_t statsIndex,
        uint64_t timestampMsec, uint64_t day,
        std::string device, uint32_t domainType, uint32_t msgType,
        void * buffer, uint32_t byteLen) {
     
    CassStatement * statement = cass_prepared_bind(prepared);
    /* Bind the values using the indices of the bind variables */
    cass_statement_bind_int64_by_name(statement, "TIMESTAMP_MSEC", timestampMsec);
    cass_statement_bind_int64_by_name(statement, "DAY", day);
    cass_statement_bind_string_by_name(statement, "DEVICE", device.c_str());
    cass_statement_bind_int32_by_name(statement, "DOMAIN_TYPE", domainType);
    cass_statement_bind_int32_by_name(statement, "MSG_TYPE", msgType);
    cass_statement_bind_bytes_by_name(statement, "DATA", (const cass_byte_t *)buffer, byteLen);
    
    CassFuture* cassQuery_future = cass_session_execute(cassSession, statement);

    /* This will block until the query has finished */
    CassError rc = cass_future_error_code(cassQuery_future);
    if(rc != CASS_OK) {
        /* count errors for each cassandra table */
        serviceSpecStatsMutex.lock();
        unsigned int * cnt = any_cast<unsigned int *>
            ((*serviceSpecStatsValues)[statsIndex]);
        *cnt++;
        serviceSpecStatsMutex.unlock();
    }
    /* Statement objects can be freed immediately after being executed */
    cass_statement_free(statement);
    cass_future_free(cassQuery_future);
}

serviceDataArchiverRabbitMQ plugin;
BOOST_DLL_AUTO_ALIAS(plugin)

}
