#!/bin/bash
#
# 本地开发环境安装脚本 (WSL/Ubuntu)
# 用于替代 Docker，更轻量的开发测试环境
#

set -e

echo "=========================================="
echo "  Base Data - 本地开发环境安装"
echo "=========================================="

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 安装目录
INSTALL_DIR="$HOME/local-dev"
mkdir -p $INSTALL_DIR
cd $INSTALL_DIR

echo -e "${YELLOW}[1/5] 安装 Java 11...${NC}"
if ! java -version 2>&1 | grep -q "11"; then
    sudo apt update
    sudo apt install -y openjdk-11-jdk
fi
echo -e "${GREEN}[OK] Java $(java -version 2>&1 | head -1)${NC}"

echo -e "${YELLOW}[2/5] 安装 Kafka 3.5.1...${NC}"
KAFKA_VERSION="3.5.1"
KAFKA_DIR="$INSTALL_DIR/kafka"
if [ ! -d "$KAFKA_DIR" ]; then
    wget -q --show-progress https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_2.12-${KAFKA_VERSION}.tgz
    tar -xzf kafka_2.12-${KAFKA_VERSION}.tgz
    mv kafka_2.12-${KAFKA_VERSION} kafka
    rm kafka_2.12-${KAFKA_VERSION}.tgz
fi
echo -e "${GREEN}[OK] Kafka installed at $KAFKA_DIR${NC}"

echo -e "${YELLOW}[3/5] 安装 ClickHouse...${NC}"
if ! command -v clickhouse-server &> /dev/null; then
    sudo apt-get install -y apt-transport-https ca-certificates dirmngr
    sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 8919F6BD2B48D754
    echo "deb https://packages.clickhouse.com/deb stable main" | sudo tee /etc/apt/sources.list.d/clickhouse.list
    sudo apt-get update
    sudo apt-get install -y clickhouse-server clickhouse-client
fi
echo -e "${GREEN}[OK] ClickHouse $(clickhouse-client --version 2>&1 | head -1)${NC}"

echo -e "${YELLOW}[4/5] 安装 sbt (Scala 构建工具)...${NC}"
if ! command -v sbt &> /dev/null; then
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add
    sudo apt-get update
    sudo apt-get install -y sbt
fi
echo -e "${GREEN}[OK] sbt $(sbt --version 2>&1 | grep sbt | head -1)${NC}"

echo -e "${YELLOW}[5/5] 创建启动脚本...${NC}"

# 创建 Kafka 启动脚本
cat > $INSTALL_DIR/start-kafka.sh << 'EOF'
#!/bin/bash
KAFKA_DIR="$HOME/local-dev/kafka"
echo "Starting Zookeeper..."
$KAFKA_DIR/bin/zookeeper-server-start.sh -daemon $KAFKA_DIR/config/zookeeper.properties
sleep 5
echo "Starting Kafka..."
$KAFKA_DIR/bin/kafka-server-start.sh -daemon $KAFKA_DIR/config/server.properties
sleep 3
echo "Kafka started! Broker: localhost:9092"
EOF
chmod +x $INSTALL_DIR/start-kafka.sh

# 创建 Kafka 停止脚本
cat > $INSTALL_DIR/stop-kafka.sh << 'EOF'
#!/bin/bash
KAFKA_DIR="$HOME/local-dev/kafka"
echo "Stopping Kafka..."
$KAFKA_DIR/bin/kafka-server-stop.sh
sleep 2
echo "Stopping Zookeeper..."
$KAFKA_DIR/bin/zookeeper-server-stop.sh
echo "Kafka stopped!"
EOF
chmod +x $INSTALL_DIR/stop-kafka.sh

# 创建 ClickHouse 启动脚本
cat > $INSTALL_DIR/start-clickhouse.sh << 'EOF'
#!/bin/bash
echo "Starting ClickHouse..."
sudo service clickhouse-server start
sleep 2
echo "ClickHouse started! HTTP: localhost:8123, Native: localhost:9000"
EOF
chmod +x $INSTALL_DIR/start-clickhouse.sh

# 创建一键启动所有服务脚本
cat > $INSTALL_DIR/start-all.sh << 'EOF'
#!/bin/bash
echo "=========================================="
echo "  启动本地开发环境"
echo "=========================================="
$HOME/local-dev/start-kafka.sh
$HOME/local-dev/start-clickhouse.sh
echo ""
echo "=========================================="
echo "  所有服务已启动!"
echo "=========================================="
echo "  Kafka:      localhost:9092"
echo "  ClickHouse: localhost:8123"
echo "=========================================="
EOF
chmod +x $INSTALL_DIR/start-all.sh

# 创建一键停止脚本
cat > $INSTALL_DIR/stop-all.sh << 'EOF'
#!/bin/bash
echo "停止所有服务..."
$HOME/local-dev/stop-kafka.sh
sudo service clickhouse-server stop
echo "所有服务已停止!"
EOF
chmod +x $INSTALL_DIR/stop-all.sh

echo ""
echo -e "${GREEN}=========================================="
echo "  安装完成!"
echo "==========================================${NC}"
echo ""
echo "使用方法:"
echo "  启动所有服务:  ~/local-dev/start-all.sh"
echo "  停止所有服务:  ~/local-dev/stop-all.sh"
echo ""
echo "单独控制:"
echo "  启动 Kafka:      ~/local-dev/start-kafka.sh"
echo "  停止 Kafka:      ~/local-dev/stop-kafka.sh"
echo "  启动 ClickHouse: ~/local-dev/start-clickhouse.sh"
echo ""
echo "验证服务:"
echo "  Kafka:      ~/local-dev/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092"
echo "  ClickHouse: clickhouse-client -q 'SELECT 1'"
echo ""
