#define BOOST_TEST_MODULE libslave
#define BOOST_TEST_DYN_LINK
#include <boost/test/unit_test.hpp>

using namespace boost;
using namespace boost::unit_test;

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/mpl/int.hpp>
#include <boost/mpl/list.hpp>
#include <boost/thread.hpp>
#include <fstream>
#include <cfloat>
#include "Slave.h"
#include "nanomysql.h"
#include "types.h"

namespace
{
    const std::string TestDataDir = "test/data/";

    struct config
    {
        std::string mysql_host;
        int         mysql_port;
        std::string mysql_db;
        std::string mysql_user;
        std::string mysql_pass;

        config()
        :   mysql_host("localhost")
        ,   mysql_port(3306)
        ,   mysql_db("test")
        ,   mysql_user("root")
        {}

        void load(const std::string& fn)
        {
            std::ifstream f(fn.c_str());
            if (!f)
                throw std::runtime_error("can't open config file '" + fn + "'");

            std::string line;
            while (getline(f, line))
            {
                if (line.empty())
                    continue;
                std::vector<std::string> tokens;
                boost::algorithm::split(tokens, line, boost::algorithm::is_any_of(" ="), boost::algorithm::token_compress_on);
                if (tokens.empty())
                    continue;
                if (tokens.size() != 2)
                    throw std::runtime_error("Malformed string '" + line + "' in the config file '" + fn + "'");

                if (tokens.front() == "mysql_host")
                    mysql_host = tokens.back();
                else if (tokens.front() == "mysql_port")
                    mysql_port = atoi(tokens.back().c_str());
                else if (tokens.front() == "mysql_db")
                    mysql_db = tokens.back();
                else if (tokens.front() == "mysql_user")
                    mysql_user = tokens.back();
                else if (tokens.front() == "mysql_pass")
                    mysql_pass = tokens.back();
                else
                    throw std::runtime_error("unknown option '" + tokens.front() + "' in config file '" + fn + "'");
            }
        }
    };

    template <typename T>
    bool not_equal(const T& a, const T& b)
    {
        return a != b;
    }

    bool not_equal(double a, double b)
    {
        return fabs(a-b) > DBL_EPSILON * fmax(fabs(a),fabs(b));
    }

    template <typename T>
    class Atomic
    {
        volatile T m_Value;

    public:
        typedef T value_type;

        Atomic() {}
        Atomic(T aValue) : m_Value(aValue) {}
        Atomic(const Atomic& r) : m_Value(r) {} // использует operator T
        Atomic& operator= (const Atomic& r) { return operator = (static_cast<T>(r)); }

        T operator++ ()         { return __sync_add_and_fetch(&m_Value, 1); }
        T operator++ (int)      { return __sync_fetch_and_add(&m_Value, 1); }
        T operator+= (T aValue) { return __sync_add_and_fetch(&m_Value, aValue); }

        T operator-- ()         { return __sync_sub_and_fetch(&m_Value, 1); }
        T operator-- (int)      { return __sync_fetch_and_sub(&m_Value, 1); }
        T operator-= (T aValue) { return __sync_sub_and_fetch(&m_Value, aValue); }

        Atomic& operator= (T aValue) { __sync_lock_test_and_set(&m_Value, aValue); return *this; }

        operator T() const
        {
            __sync_synchronize();
            return m_Value;
        }
    };

    struct Fixture
    {
        config cfg;
        slave::Slave m_Slave;
        boost::shared_ptr<nanomysql::Connection> conn;

        struct StopFlag
        {
            Atomic<int> m_StopFlag;
            Atomic<int> m_SlaveStarted;
            Atomic<int> m_SleepFlag;

            StopFlag()
            :   m_StopFlag(false)
            ,   m_SlaveStarted(false)
            ,   m_SleepFlag(false)
            {}

            bool operator() ()
            {
                m_SlaveStarted = true;
                if (m_SleepFlag)
                {
                    ::sleep(1);
                    m_SleepFlag = false;
                }
                return m_StopFlag;
            }
        };

        StopFlag        m_StopFlag;
        boost::thread   m_SlaveThread;

        struct Callback
        {
            boost::mutex m_Mutex;
            slave::callback m_Callback;
            Atomic<int> m_UnwantedCalls;

            Callback() : m_UnwantedCalls(0) {}

            void operator() (slave::RecordSet& rs)
            {
                boost::mutex::scoped_lock l(m_Mutex);
                if (!m_Callback.empty())
                    m_Callback(rs);
                else
                    ++m_UnwantedCalls;
            }

            void setCallback(slave::callback c)
            {
                boost::mutex::scoped_lock l(m_Mutex);
                m_Callback = c;
            }

            void setCallback()
            {
                boost::mutex::scoped_lock l(m_Mutex);
                m_Callback.clear();
            }
        };

        Callback m_Callback;

        void startSlave()
        {
            m_StopFlag.m_StopFlag = false;

            m_Slave.createDatabaseStructure();

            // Запускаем libslave с нашим кастомной функцией остановки, которая еще и сигнализирует,
            // когда слейв прочитал позицию бинлога и готов получать сообщения
            m_SlaveThread = boost::thread([this] () { m_Slave.get_remote_binlog(std::ref(m_StopFlag)); });

            // Ждем, чтобы libslave запустился - не более 1000 раз по 1 мс
            const timespec ts = {0 , 1000000};
            size_t i = 0;
            for (; i < 1000; ++i)
            {
                ::nanosleep(&ts, NULL);
                if (m_StopFlag.m_SlaveStarted)
                    break;
            }
            if (1000 == i)
                BOOST_FAIL ("Can't connect to mysql via libslave in 1 second");
        }

        Fixture()
        {
            cfg.load(TestDataDir + "mysql.conf");

            conn.reset(new nanomysql::Connection(cfg.mysql_host, cfg.mysql_user, cfg.mysql_pass, cfg.mysql_db));
            conn->query("set names utf8");
            // Создаем таблицу, т.к. если ее нет, libslave ругнется на ее отсутствие, и тест закончится
            conn->query("CREATE TABLE IF NOT EXISTS test (tmp int)");

            slave::MasterInfo sMasterInfo;
            sMasterInfo.host = cfg.mysql_host;
            sMasterInfo.port = cfg.mysql_port;
            sMasterInfo.user = cfg.mysql_user;
            sMasterInfo.password = cfg.mysql_pass;

            m_Slave.setMasterInfo(sMasterInfo);
            // Ставим колбек из фиксчи - а он будет вызывать колбеки, которые ему будут ставить в тестах
            m_Slave.setCallback(cfg.mysql_db, "test", boost::ref(m_Callback));
            m_Slave.init();
            startSlave();
        }

        void stopSlave()
        {
            m_StopFlag.m_StopFlag = true;
            m_Slave.close_connection();
            if (m_SlaveThread.joinable())
                m_SlaveThread.join();
        }

        ~Fixture()
        {
            stopSlave();
        }

        template<typename T>
        struct Collector
        {
            typedef boost::optional<T> Row;
            typedef std::pair<Row, Row> Event;
            typedef std::vector<Event> EventVector;
            EventVector data;

            static Row extract(const slave::Row& row)
            {
                if (row.size() > 1)
                {
                    std::ostringstream str;
                    str << "Row size is " << row.size();
                    throw std::runtime_error(str.str());
                }
                const slave::Row::const_iterator it = row.find("value");
                if (row.end() != it)
                    return boost::any_cast<T>(it->second.second);
                else
                    return Row();
            }

            void operator()(const slave::RecordSet& rs)
            {
                data.emplace_back(std::make_pair(extract(rs.m_old_row), extract(rs.m_row)));
            }
        };

        template <typename T>
        struct CheckEquality
        {
            T value;
            Atomic<int> counter;
            std::string fail_reason;

            CheckEquality(const T& t) : value(t), counter(0) {}

            void operator() (const slave::RecordSet& rs)
            {
                try
                {
                    if (++counter > 1)
                        throw std::runtime_error("Second call on CheckEquality");
                    const slave::Row& row = rs.m_row;
                    if (row.size() > 1)
                    {
                        std::ostringstream str;
                        str << "Row size is " << row.size();
                        throw std::runtime_error(str.str());
                    }
                    const slave::Row::const_iterator it = row.find("value");
                    if (row.end() == it)
                        throw std::runtime_error("Can't find field 'value' in the row");
                    const T t = boost::any_cast<T>(it->second.second);
                    //if (value != t)
                    if (not_equal(value,t))
                    {
                        std::ostringstream str;
                        str << "Value '" << value << "' is not equal to libslave value '" << t << "'";
                        throw std::runtime_error(str.str());
                    }

                }
                catch (const std::exception& ex)
                {
                    fail_reason += '\n';
                    fail_reason += ex.what();
                }
            }
        };

        template<typename CheckF> void wait(CheckF f, const std::string& aErrorMsg)
        {
            const timespec ts = {0 , 1000000};
            size_t i = 0;
            for (; i < 1000; ++i)
            {
                ::nanosleep(&ts, NULL);
                if (f())
                    break;
            }
            if (!f())
                BOOST_ERROR ("Have no calls to libslave callback for 1 second: " << aErrorMsg);
        }

        template<typename T, typename F>
        void check(F f, const std::string& aQuery, const std::string& aErrorMsg)
        {
            // Устанавливаем в libslave колбек для проверки этого значения
            Collector<T> sCallback;
            m_Callback.setCallback(std::ref(sCallback));
            // Проверяем, что не было нежелательных вызовов до этого
            if (0 != m_Callback.m_UnwantedCalls)
                BOOST_ERROR("Unwanted calls before this case: " << m_Callback.m_UnwantedCalls << aErrorMsg);

            // Модифицируем таблицу
            conn->query(aQuery);

            // Ждем отработки колбека максимум 1 секунду
            wait([&sCallback](){ return !sCallback.data.empty(); }, aErrorMsg);

            f(sCallback.data);

            // Убираем наш колбек, т.к. он при выходе из блока уничтожится, заодно чтобы
            // строку он не мучал больше, пока мы ее проверяем
            m_Callback.setCallback();
        }

        template <typename T>
        void checkInsertValue(T t, const std::string& aInsertString, const std::string& aErrorMsg)
        {
            check<T>([&t, &aErrorMsg](const typename Collector<T>::EventVector& data)
            {
                if (data.size() != 1)
                    BOOST_ERROR ("Have invalid call count: " << data.size() << " for " << aErrorMsg);
                const auto& pair = data.front();
                if (pair.first)
                    BOOST_ERROR("Has before image for insert: '" << pair.first.get()
                                << "' while has expecting nothing during" << aErrorMsg);
                if (!pair.second)
                    BOOST_ERROR("Has not after image for insert while has expecting: '" << t
                                << "' during " << aErrorMsg);
                if (not_equal(pair.second.get(), t))
                    BOOST_ERROR("Has invalid image for insert: '" << pair.first.get()
                                << "' while has expecting: '" << t
                                << "' during " << aErrorMsg);
            }, "INSERT INTO test VALUES (" + aInsertString + ")", aErrorMsg);
        }

        template<typename T>
        struct Line
        {
            std::string type;
            std::string filename;
            std::string line;
            size_t      lineNumber;
            std::string insert;
            T           expected;
        };

        template<typename T> void recreate(boost::shared_ptr<nanomysql::Connection>& conn,
                                           const Line<T>& c)
        {
            const std::string sDropTableQuery = "DROP TABLE IF EXISTS test";
            conn->query(sDropTableQuery);
            const std::string sCreateTableQuery = "CREATE TABLE test (value " + c.type + ") DEFAULT CHARSET=utf8";
            conn->query(sCreateTableQuery);
        }

        template<typename T> std::string errorMessage(const Line<T>& c)
        {
            return "(we are now on file '" + c.filename + "' line " + std::to_string(c.lineNumber) + ": '" + c.line + "')";
        }

        template<typename T> void testInsert(boost::shared_ptr<nanomysql::Connection>& conn,
                                             const std::vector<Line<T>>& data,
                                             slave::EventKind flag)
        {
            for (const Line<T>& c : data)
            {
                recreate(conn, c);
                checkInsertValue<T>(c.expected, c.insert, errorMessage(c));
            }
        }

        template<typename T> void testUpdate(boost::shared_ptr<nanomysql::Connection>& conn,
                                             const std::vector<Line<T>>& data,
                                             slave::EventKind flag)
        {
        }

        template<typename T> void testDelete(boost::shared_ptr<nanomysql::Connection>& conn,
                                             const std::vector<Line<T>>& data,
                                             slave::EventKind flag)
        {
        }

        template<typename T> void testAll(boost::shared_ptr<nanomysql::Connection>& conn,
                                          const std::vector<Line<T>>& data,
                                          slave::EventKind flag)
        {
            testInsert(conn, data, flag);
            testUpdate(conn, data, flag);
            testDelete(conn, data, flag);
        }

        template<typename T> void testAll(boost::shared_ptr<nanomysql::Connection>& conn,
                                          const std::vector<Line<T>>& data)
        {
            if (data.empty())
                return;
            testAll(conn, data, slave::eAll);
            testAll(conn, data, slave::eInsert);
            testAll(conn, data, slave::eUpdate);
            testAll(conn, data, slave::eDelete);
            testAll(conn, data, static_cast<slave::EventKind>(~static_cast<uint8_t>(slave::eInsert)));
            testAll(conn, data, static_cast<slave::EventKind>(~static_cast<uint8_t>(slave::eUpdate)));
            testAll(conn, data, static_cast<slave::EventKind>(~static_cast<uint8_t>(slave::eDelete)));
            testAll(conn, data, slave::eNone);
        }
    };

    BOOST_AUTO_TEST_SUITE(SlaveConf)

    BOOST_AUTO_TEST_CASE(test_HelloWorld)
    {
        std::cout << "You probably should specify parameters to mysql in the file " << TestDataDir << "mysql.conf first" << std::endl;
    }

    BOOST_AUTO_TEST_SUITE_END()

    BOOST_FIXTURE_TEST_SUITE(Slave, Fixture)

    enum MYSQL_TYPE
    {
        MYSQL_TINYINT,
        MYSQL_INT,
        MYSQL_BIGINT,
        MYSQL_CHAR,
        MYSQL_VARCHAR,
        MYSQL_TINYTEXT,
        MYSQL_TEXT,
        MYSQL_DECIMAL,
        MYSQL_BIT,
        MYSQL_SET
    };

    template <MYSQL_TYPE T>
    struct MYSQL_type_traits;

    template <>
    struct MYSQL_type_traits<MYSQL_INT>
    {
        typedef slave::types::MY_INT slave_type;
        static const std::string name;
    };
    const std::string MYSQL_type_traits<MYSQL_INT>::name = "INT";

    template <>
    struct MYSQL_type_traits<MYSQL_BIGINT>
    {
        typedef slave::types::MY_BIGINT slave_type;
        static const std::string name;
    };
    const std::string MYSQL_type_traits<MYSQL_BIGINT>::name = "BIGINT";

    template <>
    struct MYSQL_type_traits<MYSQL_CHAR>
    {
        typedef slave::types::MY_CHAR slave_type;
        static const std::string name;
    };
    const std::string MYSQL_type_traits<MYSQL_CHAR>::name = "CHAR";

    template <>
    struct MYSQL_type_traits<MYSQL_VARCHAR>
    {
        typedef slave::types::MY_VARCHAR slave_type;
        static const std::string name;
    };
    const std::string MYSQL_type_traits<MYSQL_VARCHAR>::name = "VARCHAR";

    template <>
    struct MYSQL_type_traits<MYSQL_TINYTEXT>
    {
        typedef slave::types::MY_TINYTEXT slave_type;
        static const std::string name;
    };
    const std::string MYSQL_type_traits<MYSQL_TINYTEXT>::name = "TINYTEXT";

    template <>
    struct MYSQL_type_traits<MYSQL_TEXT>
    {
        typedef slave::types::MY_TEXT slave_type;
        static const std::string name;
    };
    const std::string MYSQL_type_traits<MYSQL_TEXT>::name = "TEXT";

    template <>
    struct MYSQL_type_traits<MYSQL_DECIMAL>
    {
        typedef slave::types::MY_DECIMAL slave_type;
        static const std::string name;
    };
    const std::string MYSQL_type_traits<MYSQL_DECIMAL>::name = "DECIMAL";

    template <>
    struct MYSQL_type_traits<MYSQL_BIT>
    {
        typedef slave::types::MY_BIT slave_type;
        static const std::string name;
    };
    const std::string MYSQL_type_traits<MYSQL_BIT>::name = "BIT";

    template <>
    struct MYSQL_type_traits<MYSQL_SET>
    {
        typedef slave::types::MY_SET slave_type;
        static const std::string name;
    };
    const std::string MYSQL_type_traits<MYSQL_SET>::name = "SET";

    template <typename T>
    void getValue(const std::string& s, T& t)
    {
        std::istringstream is;
        is.str(s);
        is >> t;
    }

    void getValue(const std::string& s, std::string& t)
    {
        t = s;
        // Убираем ведущий пробел
        t.erase(0, 1);
    }

    typedef boost::mpl::list<
        boost::mpl::int_<MYSQL_INT>,
        boost::mpl::int_<MYSQL_BIGINT>,
        boost::mpl::int_<MYSQL_CHAR>,
        boost::mpl::int_<MYSQL_VARCHAR>,
        boost::mpl::int_<MYSQL_TINYTEXT>,
        boost::mpl::int_<MYSQL_TEXT>,
        boost::mpl::int_<MYSQL_DECIMAL>,
        boost::mpl::int_<MYSQL_BIT>,
        boost::mpl::int_<MYSQL_SET>
    > mysql_one_field_types;

    BOOST_AUTO_TEST_CASE_TEMPLATE(test_OneField, T, mysql_one_field_types)
    {
        typedef MYSQL_type_traits<MYSQL_TYPE(T::value)> type_traits;
        typedef typename type_traits::slave_type slave_type;

        const std::string sDataFilename = TestDataDir + "OneField/" + type_traits::name;
        std::ifstream f(sDataFilename.c_str());
        BOOST_REQUIRE_MESSAGE(f, "Cannot open file with data: '" << sDataFilename << "'");
        std::string line;
        size_t line_num = 0;
        std::vector<Line<slave_type>> data;
        std::string type;
        while (getline(f, line))
        {
            ++line_num;
            if (line.empty())
                continue;
            std::vector<std::string> tokens;
            const char* sDelimiters = ",";
            if ("SET" == type_traits::name)
                sDelimiters=";";
            boost::algorithm::split(tokens, line, boost::algorithm::is_any_of(sDelimiters), boost::algorithm::token_compress_on);
            if (tokens.empty())
                continue;
            if (tokens.front() == "define")
            {
                if (tokens.size() > 2)
                {
                    std::string dec = tokens[1].substr(1, tokens[1].find('(', 0)-1);
                    if ("DECIMAL" == dec)
                    {
                        tokens[1] += "," + tokens[2];
                        tokens.pop_back();
                    }
                }
                if (tokens.size() != 2)
                    BOOST_FAIL("Malformed string '" << line << "' in the file '" << sDataFilename << "'");
                type = tokens[1];
                testAll(conn, data);
                data.clear();
            }
            else if (tokens.front() == "data")
            {
                if (tokens.size() != 3)
                    BOOST_FAIL("Malformed string '" << line << "' in the file '" << sDataFilename << "'");

                // Получаем значение, с которым надо будет сравнить значение из libslave
                slave_type checked_value;
                getValue(tokens[2], checked_value);

                Line<slave_type> current;
                current.type = type;
                current.filename = sDataFilename;
                current.line = line;
                current.lineNumber = line_num;
                current.insert = tokens[1];
                current.expected = checked_value;
                data.push_back(current);
            }
            else if (tokens.front()[0] == ';')
                continue;   // комментарий
            else
                BOOST_FAIL("Unknown command '" << tokens.front() << "' in the file '" << sDataFilename << "' on line " << line_num);
        }
        testAll(conn, data);
        data.clear();
    }

    // Проверяем, что если останавливаем слейв, он в дальнейшем продолжит читать с той же позиции
    BOOST_AUTO_TEST_CASE(test_StartStopPosition)
    {
        // Создаем нужную таблицу
        conn->query("DROP TABLE IF EXISTS test");
        conn->query("CREATE TABLE IF NOT EXISTS test (value int)");

        checkInsertValue(uint32_t(12321), "12321", "");

        stopSlave();

        conn->query("INSERT INTO test VALUES (345234)");

        CheckEquality<uint32_t> sCallback(345234);
        m_Callback.setCallback(std::ref(sCallback));

        startSlave();

        // Ждем отработки колбека максимум 1 секунду
        const timespec ts = {0 , 1000000};
        size_t i = 0;
        for (; i < 1000; ++i)
        {
            ::nanosleep(&ts, NULL);
            if (sCallback.counter >= 1)
                break;
        }
        if (sCallback.counter < 1)
            BOOST_ERROR ("Have no calls to libslave callback for 1 second");

        // Убираем наш колбек, т.к. он при выходе из блока уничтожится, заодно чтобы
        // строку он не мучал больше, пока мы ее проверяем
        m_Callback.setCallback();

        if (!sCallback.fail_reason.empty())
            BOOST_ERROR(sCallback.fail_reason << "\n");

        // Проверяем, что не было нежелательных вызовов до этого
        if (0 != m_Callback.m_UnwantedCalls)
            BOOST_ERROR("Unwanted calls before this case: " << m_Callback.m_UnwantedCalls);
    }

    struct CheckBinlogPos
    {
        const slave::Slave& m_Slave;
        slave::Slave::binlog_pos_t m_LastPos;

        CheckBinlogPos(const slave::Slave& aSlave, const slave::Slave::binlog_pos_t& aLastPos)
        :   m_Slave(aSlave), m_LastPos(aLastPos)
        {}

        bool operator() () const
        {
            const slave::MasterInfo& sMasterInfo = m_Slave.masterInfo();
            if (sMasterInfo.master_log_name > m_LastPos.first
            || (sMasterInfo.master_log_name == m_LastPos.first
                && sMasterInfo.master_log_pos >= m_LastPos.second))
                return true;
            return false;
        }
    };

    struct CallbackCounter
    {
        Atomic<int> counter;
        std::string fail;

        CallbackCounter() : counter(0) {}

        void operator() (const slave::RecordSet& rs)
        {
            if (++counter > 2)
                fail = std::to_string(counter) + " calls on CallbackCounter";
        }
    };

    // Проверяем, работает ли ручное выставление позиции бинлога
    BOOST_AUTO_TEST_CASE(test_SetBinlogPos)
    {
        // Создаем нужную таблицу
        conn->query("DROP TABLE IF EXISTS test");
        conn->query("CREATE TABLE IF NOT EXISTS test (value int)");

        checkInsertValue(uint32_t(12321), "12321", "");

        // Запоминаем позицию
        const slave::Slave::binlog_pos_t sInitialBinlogPos = m_Slave.getLastBinlog();

        // Вставляем значение, читаем его
        checkInsertValue(uint32_t(12322), "12322", "");

        stopSlave();

        // Вставляем новое значение
        conn->query("INSERT INTO test VALUES (345234)");

        // И получаем новую позицию
        const slave::Slave::binlog_pos_t sCurBinlogPos = m_Slave.getLastBinlog();
        BOOST_CHECK_NE(sCurBinlogPos.second, sInitialBinlogPos.second);

        // Теперь выставляем в слейв старую позицию и проверяем, что 2 INSERTа прочтутся (12322 и 345234)
        slave::MasterInfo sMasterInfo = m_Slave.masterInfo();
        sMasterInfo.master_log_name = sInitialBinlogPos.first;
        sMasterInfo.master_log_pos = sInitialBinlogPos.second;
        m_Slave.setMasterInfo(sMasterInfo);

        CallbackCounter sCallback;
        m_Callback.setCallback(std::ref(sCallback));
        if (0 != m_Callback.m_UnwantedCalls)
        {
            BOOST_ERROR("Unwanted calls before this case: " << m_Callback.m_UnwantedCalls);
        }

        m_SlaveThread = boost::thread([this, sCurBinlogPos] () { m_Slave.get_remote_binlog(CheckBinlogPos(m_Slave, sCurBinlogPos)); });

        // Ждем отработки колбека максимум 1 секунду
        const timespec ts = {0 , 1000000};
        size_t i = 0;
        for (; i < 1000; ++i)
        {
            ::nanosleep(&ts, NULL);
            if (sCallback.counter >= 2)
                break;
        }
        if (sCallback.counter < 2)
            BOOST_ERROR ("Have less than two calls to libslave callback for 1 second");

        // Убираем наш колбек, т.к. он при выходе из блока уничтожится, заодно чтобы
        // строку он не мучал больше, пока мы ее проверяем
        m_Callback.setCallback();

        if (!sCallback.fail.empty())
            BOOST_ERROR(sCallback.fail);

        BOOST_CHECK_MESSAGE (m_SlaveThread.joinable(), "m_Slave.get_remote_binlog is not finished yet and will be never!");
    }

    // Проверяем, что если соединение с базой рвется (без выхода из get_remote_binlog), то начинаем читать оттуда, где остановились
    BOOST_AUTO_TEST_CASE(test_Disconnect)
    {
        // Создаем нужную таблицу
        conn->query("DROP TABLE IF EXISTS test");
        conn->query("CREATE TABLE IF NOT EXISTS test (value int)");

        checkInsertValue(uint32_t(12321), "12321", "");

        m_StopFlag.m_SleepFlag = true;
        m_Slave.close_connection();

        conn->query("INSERT INTO test VALUES (345234)");

        CheckEquality<uint32_t> sCallback(345234);
        m_Callback.setCallback(std::ref(sCallback));

        // Ждем отработки колбека максимум 2 секунды (потому что одну спит колбек перед реконнектом)
        const timespec ts = {0 , 1000000};
        size_t i = 0;
        for (; i < 2000; ++i)
        {
            ::nanosleep(&ts, NULL);
            if (sCallback.counter >= 1)
                break;
        }
        if (sCallback.counter < 1)
            BOOST_ERROR ("Have no calls to libslave callback for 2 seconds");

        // Убираем наш колбек, т.к. он при выходе из блока уничтожится, заодно чтобы
        // строку он не мучал больше, пока мы ее проверяем
        m_Callback.setCallback();

        if (!sCallback.fail_reason.empty())
            BOOST_ERROR(sCallback.fail_reason << "\n");

        // Проверяем, что не было нежелательных вызовов до этого
        if (0 != m_Callback.m_UnwantedCalls)
            BOOST_ERROR("Unwanted calls before this case: " << m_Callback.m_UnwantedCalls);
    }

    BOOST_AUTO_TEST_SUITE_END()
}// anonymous-namespace
