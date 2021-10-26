# One api
export ONEAPI_MPICH_OVERRIDE=NONE   
module use /soft/restricted/CNDA/modulefiles
module load oneapi
# module load oneapi/2021.04.30.004
export SYCL_DEVICE_FILTER=opencl
