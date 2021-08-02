package com.foxconn.ipebg.databus.kafka.runner;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Date;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

@Configuration
@Slf4j
@EqualsAndHashCode(callSuper = false)
@Data
public class PostgresPuller extends Thread {

    private final static Charset charset = Charset.forName("UTF-8");
    public static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMM");

    @Value("${postgresql.slot.name}")
    private String slotName;
    @Value("${postgresql.url}")
    private String url;
    @Value("${postgresql.slot.actions}")
    private String actions;
    @Value("${system.name}")
    private String topicPrefix;
    @Autowired
    private KafkaTemplate<Long, String> producer;

    @Resource
    private CuratorFramework curatorClient;
    @Value("${zookeeper.path}")
    private String path;
    @Value("${max.waiting-counter}")
    private long maxWaiting;

    private volatile boolean isRunning = false;

    private LongAdder totalCount = new LongAdder();

    private LongAdder lastCnt = new LongAdder();

    private LongAdder succCounter = new LongAdder();

    private LongAdder failCounter = new LongAdder();

    ExecutorService pool = Executors.newSingleThreadExecutor();

    private Map<String, LongAdder> counterMap = new HashMap<>();
    private Map<String, Map<String, LongAdder>> tableMap = new HashMap<>();

    private ConcurrentHashMap<String, String> tableToTopic = new ConcurrentHashMap<>();

    private final Gson gson = new Gson();

    private LongAdder waitingCounter = new LongAdder();

    private AtomicBoolean isWaiting = new AtomicBoolean(true);
    private volatile boolean isShutdown = false;
    private volatile boolean needPause = false;
    private volatile int mapSize = 0;

    @Override
    public void run() {
        while (true && !isShutdown) {
            if (isRunning) {
                System.out.println("111111==========================="+new Date());
                ScheduledExecutorService schedule = Executors.newScheduledThreadPool(1);
                mapSize = tableToTopic.size();
                schedule.scheduleAtFixedRate(() -> {
                    if (isRunning && !isWaiting.get()) {
                        log.info("totoal :" + totalCount.longValue() + ";delta:" + (totalCount.longValue() - lastCnt.longValue()));
                        log.info("success Send :" + succCounter.longValue() + ";failed send:" + failCounter.longValue());
                        lastCnt.reset();
                        lastCnt.add(totalCount.longValue());
                        refreshTableTopic();
                        if (mapSize != tableToTopic.size()) {
                            mapSize = tableToTopic.size();
                            needPause = true;
                        }
                    }
                }, 1, 1, TimeUnit.MINUTES);
                System.out.println("22222==========================="+new Date());
                pullEvents();
                schedule.shutdown();
            } else {
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        pool.shutdown();
        while (!pool.isTerminated()) {
            log.info("shutdown postgresPuller,waiting 10s");
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void pullEvents() {
        try (Connection conn = getConnection();
             PGReplicationStream stream = getEventStream(conn);) {
            System.out.println("======================== "+new Date());
            LongAdder oneTranscationCount = new LongAdder();
            while (isRunning && !needPause) {
                if (stream == null) {//当前slot正在被使用
                    isWaiting.set(true);
                    TimeUnit.SECONDS.sleep(10);
                    break;
                } else {
                    isWaiting.set(false);
                }
                if (waitingCounter.longValue() > maxWaiting) {//当前堆积数据过多
                    TimeUnit.SECONDS.sleep(1);
                    continue;
                }
                ByteBuffer buffer = stream.readPending();
                LogSequenceNumber lsn = stream.getLastReceiveLSN();
                if (null == buffer) {
                    log.debug("null data,lsn={}", lsn.toString());
                    try {
                        TimeUnit.MILLISECONDS.sleep(1);
                        stream.setAppliedLSN(lsn);
                        stream.setFlushedLSN(lsn);
                    } catch (InterruptedException e) {

                    }
                    continue;
                }
                pool.submit(() -> {
                    buffer.clear();
                    CharBuffer chars = charset.decode(buffer);
                    String event = chars.toString();
                    JsonObject obj = gson.fromJson(event, JsonObject.class);
                    String action = obj.getAsJsonPrimitive("action").getAsString();
                    switch (action) {
                        case "B": {// 事务开始
                            oneTranscationCount.reset();
                            break;
                        }
                        case "C": {// 事务结束
                            stream.setAppliedLSN(lsn);
                            stream.setFlushedLSN(lsn);
                            if (oneTranscationCount.longValue() > 5000) {
                                log.info("bigTranscation lsn" + lsn + ",have data :" + oneTranscationCount);
                            }
                            break;
                        }
                        case "I":
                        case "D":
                        case "U": {
                            // action =I,U,D执行这段代码并退出
//						String table = obj.getAsJsonPrimitive("table").getAsString();
                            String table = obj.getAsJsonPrimitive("table").getAsString().replaceAll("_\\d{4,}$", "");
                            String topic = getTopic(table);
                            if (topic == null) {
                                return;
                            }
                            obj.addProperty("table", table);
                            totalCount.increment();
                            waitingCounter.increment();
                            producer.send(topic, obj.toString()).addCallback(result -> {
                                succCounter.increment();
                                waitingCounter.decrement();
                            }, ex -> {
                                failCounter.increment();
                                waitingCounter.decrement();
                                log.error(ex.getMessage());
                            });
                            LongAdder counter = counterMap.getOrDefault(topic, new LongAdder());
                            counter.increment();
                            if (counter.longValue() == 1) {
                                counterMap.put(topic, counter);
                            }
                            Map<String, LongAdder> tableCounter = tableMap.get(table);
                            if (tableCounter == null) {
                                tableCounter = new HashMap<String, LongAdder>();
                                tableCounter.put("I", new LongAdder());
                                tableCounter.put("U", new LongAdder());
                                tableCounter.put("D", new LongAdder());
                                tableMap.put(table, tableCounter);
                            }
                            tableCounter.get(action).increment();
                            oneTranscationCount.increment();
                            break;
                        }
                        default: {// 事务提交的数据变更
                            log.info("unsupported action:" + obj.getAsJsonPrimitive("action").getAsString());
                            break;
                        }
                    }
                });
            }
        } catch (Exception e1) {
            e1.printStackTrace();
        }
    }

    private Connection getConnection() {
        if (url == null || "".equals(url)) {
            log.error("postgresql.url can't be null.");
            System.exit(0);
        }
        Properties props = new Properties();
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");
        PGProperty.RECEIVE_BUFFER_SIZE.set(props, "" + Integer.MAX_VALUE);
        PGProperty.SEND_BUFFER_SIZE.set(props, "" + Integer.MAX_VALUE);
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, props);
        } catch (Exception e) {
            conn = null;
            e.printStackTrace();
        }
        return conn;
    }

    public synchronized void startPuller() {
        if (isRunning) {
            return;
        }
        isRunning = true;
    }

    public Map<String, LongAdder> getCounter() {
        return counterMap;
    }

    public synchronized void stopPuller() {
        if (isRunning) {
            isRunning = false;
        }
    }

    public synchronized void shutDownPuller() {
        if (isRunning) {
            isRunning = false;
        }
        isShutdown = true;
    }

    //将普通链接转换为逻辑流复制链接
    private PGReplicationStream getEventStream(Connection conn) throws SQLException {
        createSlot();
        PGReplicationStream stream = null;
        if (isSlotActive()) {
            return stream;
        }
        StringBuilder sb = new StringBuilder();
        refreshTableTopic();
        tableToTopic.keySet().forEach(table -> {
            sb.append(',').append('*').append('.').append(table);
        });
        sb.deleteCharAt(0);
        log.info("add-tables:={}", sb.toString());
        PGConnection pgConn = conn.unwrap(PgConnection.class);
        ChainedLogicalStreamBuilder builder = pgConn.getReplicationAPI().replicationStream().logical()
                .withSlotName(slotName).withSlotOption("include-types", false).withSlotOption("include-typmod", false)
                .withSlotOption("format-version", "2").withSlotOption("actions", actions)
                .withSlotOption("add-tables", sb.toString())
                .withStatusInterval(10, TimeUnit.SECONDS);
        stream = builder.start();
        needPause = false;
        log.info("get Replication Stream successful.");
        return stream;
    }

    public void createSlot() {
        StringBuilder sb = new StringBuilder(
                "select pg_create_logical_replication_slot('" + slotName + "', 'wal2json')\r\n");
        sb.append("where not exists (select * from pg_replication_slots where slot_name='" + slotName + "')");
        try (Connection conn = getConnection();
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sb.toString());) {
            if (rs.next()) {
                log.info(slotName + " is not exist in " + conn.getMetaData().getURL());
                log.info(slotName + " is created with start lsn " + rs.getString(1));
            } else {
                log.info(slotName + " is exist in " + conn.getMetaData().getURL());
            }
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("can not create or query slot.");
        }
    }

    private boolean isSlotActive() {
        StringBuilder sb = new StringBuilder();
        sb.append("select * from pg_replication_slots where slot_name='" + slotName + "' and active='t' and active_pid is not null");
        try (Connection conn = getConnection();
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sb.toString());) {
            if (rs.next()) {
                log.info(slotName + " is active now,wait 10 second.");
                TimeUnit.SECONDS.sleep(10);
                return true;
            } else {

                log.info(slotName + " is not active,will start read this slot.");
                return false;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("can not create or query slot.");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return true;
    }

    @Override
    public String toString() {
        return "PostgresPuller [slotName=" + slotName + ", url=" + url + "]";
    }

    public String getTopic(String table) {
        String topic = tableToTopic.get(table);
        if (topic == null) {
            for (String temp : tableToTopic.keySet()) {
                if (table.matches(temp + "_\\d+$")) {
                    topic = tableToTopic.get(temp);
                    setTopic(table, topic);
                    break;
                }
            }
        }
        return topic;
    }

    private void setTopic(String table, String topic) {
        String realTopic = topic.startsWith(topicPrefix + '.') ? topic : topicPrefix + "." + topic;
        if (table.matches(".*_\\d+$")) {
            tableToTopic.put(table, realTopic);
        } else {
            LocalDate now = LocalDate.now();
            for (int i = 4; i < 24; i++) {
                tableToTopic.remove(table + '_' + now.minusMonths(i).format(DATE_FORMAT));
            }
            tableToTopic.put(table, realTopic);
            tableToTopic.put(table + '_' + now.minusMonths(3).format(DATE_FORMAT), realTopic);
            tableToTopic.put(table + '_' + now.minusMonths(2).format(DATE_FORMAT), realTopic);
            tableToTopic.put(table + '_' + now.minusMonths(1).format(DATE_FORMAT), realTopic);
            tableToTopic.put(table + '_' + now.format(DATE_FORMAT), realTopic);
            tableToTopic.put(table + '_' + now.plusMonths(1).format(DATE_FORMAT), realTopic);
        }
    }

    private void refreshTableTopic() {
        //记录刷新前的数据
        ConcurrentHashMap<String, String> tableToTopicOld =  (ConcurrentHashMap<String, String>) SerializationUtils.clone(tableToTopic);

        Iterator<Map.Entry<String, String>> iter = tableToTopic.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            setTopic(entry.getKey(), entry.getValue());
        }

        //刷新完毕，比较是否不同，如果不同，重新加载流
        for(String table : tableToTopic.keySet()){
            if(tableToTopicOld.get(table) == null){
                needPause = true;
                break;
            }
        }
    }

    private void removeTable(String table) {
        Iterator<Map.Entry<String, String>> iter = tableToTopic.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            if (entry.getKey().matches(table + "(_\\d+)?$")) {
                iter.remove();
            }
        }
    }

    @PostConstruct
    public void addTopicListener() {
        CuratorCache nodeCache = CuratorCache.build(curatorClient, path);
        nodeCache.listenable().addListener((type, oldData, data) -> {
            log.info("type:=" + type);
            if (oldData != null) {
                log.info("oldData:=" + oldData.toString());
            }
            if (data != null) {
                log.info("data:=" + data.toString());
            }
            switch (type) {
                case NODE_CHANGED: {
                    if (!path.equals(data.getPath())) {
                        String table = data.getPath().replace(path + '/', "");
                        String topic = new String(data.getData());

                        setTopic(table, topic);
                    }
                    break;
                }
                case NODE_CREATED: {
                    if (!path.equals(data.getPath())) {
                        String table = data.getPath().replace(path + '/', "");
                        String topic = new String(data.getData());
                        System.out.println("333333==========================="+new Date());
                        setTopic(table, topic);
                        needPause = true;
                    }
                    break;
                }
                case NODE_DELETED: {
                    String table = oldData.getPath().replace(path + '/', "");
                    removeTable(table);
                    System.out.println("444444444444==========================="+new Date());
                    needPause = true;
                    break;
                }
                default:
                    break;
            }
        });
        nodeCache.start();
    }

    public List<Map<String, String>> getSlotSize() {
        List<Map<String, String>> list = new ArrayList<>();
        StringBuilder sb = new StringBuilder("select slot_name as name, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn)) as size from pg_replication_slots");

        try (Connection conn = getConnection();
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sb.toString())) {

            while (rs.next()) {
                Map<String, String> map = new HashMap<>();
                String name = rs.getString(1);
                String size = rs.getString(2);
                map.put("name", name);
                map.put("size", size);
                list.add(map);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("can not query slot size.");
        }
        return list;
    }

    public Map<String, Map<String, LongAdder>> getTableMap() {
        return tableMap;
    }
}
