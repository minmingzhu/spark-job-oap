module load oneapi/release/2024.2.1
ml use /opt/aurora/24.347.0/spack/unified/0.9.2/install/modulefiles/Core
ml gcc/13.3.0
source /lus/flare/projects/Aurora_deployment/spark/oneCCL/build/_install/env/vars.sh --ccl-bundled-mpi=no
SECOND_IP=$(hostname -I | awk '{print $2}')
export SPARK_LOCAL_IP=$SECOND_IP
export CCL_TOPO_FABRIC_VERTEX_CONNECTION_CHECK=0
export CCL_ATL_TRANSPORT=ofi
export ONEAPI_DEVICE_SELECTOR=level_zero:*
export ZE_FLAT_DEVICE_HIERARCHY=FLAT
export FI_CXI_DEFAULT_CQ_SIZE=1048576
export FI_UNIVERSE_SIZE=1024
export FI_CXI_RX_MATCH_MODE=hybrid
#export CCL_LOG_LEVEL=debug
export FI_CXI_OPTIMIZED_MRS=0
export FI_CXI_REQ_BUF_MIN_POSTED=8
export FI_CXI_REQ_BUF_SIZE=8388608
export FI_CXI_OFLOW_BUF_SIZE=8388608
export CCL_ZE_IPC_EXCHANGE=sockets
export CCL_KVS_GET_TIMEOUT=1200
export ZE_ENABLE_PCI_ID_DEVICE_ORDER=1

