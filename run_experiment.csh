#!/bin/csh

set base=/scratch/01479/tsupine/24May2011/
set job=3km-MPAR-fcst-2100

set n_ens=40
set mpi_config="2 15"

#python run_experiment.py  \
#      --base-path $base  --job-name $job                                                                                      \
#      --n-ens $n_ens  --mpi-config $mpi_config  --algorithm ensrf                                                             \
#      --ens-start 0  --ens-end 10800  --ens-step 3600  --assim-step 3600  --chunk-size 1800                                   \
#      --arps-template arps.3km.input  --arpsenkf-template arpsenkf.3km.input  --arpsenkfic-template arpsenkfic.3km.input      \
#      --assim-radar 3kmelreno.radflag  --assim-prof no  --assim-surf yes                                                      \
#      --covariance-inflation 0:multd=1.05 10800:mults=1.20,adapt=0.90                                                         \
#      --fcst-req 0:25  --init-fcst-req 0:50  --assim-on-req 1:30                                                              \
#      --restart --debug

#python run_experiment.py \
#       --base-path $base  --job-name $job                                                                                      \
#       --n-ens $n_ens  --mpi-config $mpi_config  --algorithm ensrf                                                             \
#       --ens-start 10800  --ens-end 18000  --ens-step 300  --assim-step 300  --chunk-size 300                                  \
#       --arps-template arps.3km.input  --arpsenkf-template arpsenkf.3km.input  --arpsenkfic-template arpsenkfic.3km.input      \
#       --assim-radar 3kmelreno.KOUN.radflag  --assim-prof no                                                                   \
#       --covariance-inflation 10800:mults=1.20,adapt=0.90                                                                      \
#       --fcst-req 0:20  --init-fcst-req 0:40  --assim-off-req 1:15  --assim-on-req 1:30                                        \
#       --debug  --restart

#python run_experiment.py \
#      --base-path $base  --job-name $job                                                                                      \
#      --n-ens $n_ens  --mpi-config $mpi_config  --algorithm ensrf                                                             \
#      --ens-start 18000  --ens-end 21600  --ens-step 300  --assim-step 300  --chunk-size 300                                  \
#      --arps-template arps.3km.input  --arpsenkf-template arpsenkf.3km.input  --arpsenkfic-template arpsenkfic.3km.input      \
#      --assim-radar 3kmelreno.KOUN.radflag  --assim-prof no                                                                   \
#      --covariance-inflation 10800:mults=1.20,adapt=0.90                                                                      \
#      --fcst-req 0:20  --init-fcst-req 0:40  --assim-off-req 1:15  --assim-on-req 1:30                                        \
#      --debug  --restart

#### 3km forecasts
#python run_experiment.py \
#      --base-path $base  --job-name $job                                                                \
#      --n-ens $n_ens  --mpi-config $mpi_config                                                          \
#      --ens-start 21600  --ens-end 25200  --ens-step 300  --assim-step 300  --chunk-size 3600           \
#      --arps-template arps.3km.input                                                                    \
#      --fcst-req 0:45                                                                                   \
#      --free-forecast  --debug  --restart  --job-grouping integration

#python run_experiment.py  \
#    --base-path /lustre/scratch/tsupinie/24May2011/  --job-name 3km-initial                                                 \
#    --members 39 40  --n-cores 48  --mpi-config 2 12                                                                        \
#    --ens-start 0  --ens-end 3600  --ens-step 3600  --assim-step 3600  --chunk-size 1800                                    \
#    --arps-template arps.3km.input  --arpsenkf-template arpsenkf.3km.input  --arpsenkfic-template arpsenkfic.3km.input      \
#    --fcst-req 0:45  --init-fcst-req 1:30                                                                                   \
#    --debug  --restart --free-forecast

python joinsplit.py \
      --base-path $base  --job-name $job                     \
      --n-ens $n_ens  --mpi-config $mpi_config               \
      --ens-start 21600  --ens-end 25200  --ens-step 300     \
      --time-req 00:30  --job-grouping yes

#python joinsplit.py \
#    --base-path $base  --job-name $job                     \
#    --n-ens $n_ens  --mpi-config $mpi_config               \
#    --ens-start 0  --ens-end 10800  --ens-step 1800        \
#    --time-req 00:30  --forecast

########## 1 km legacy experiments
# Start a new run
#python run_experiment.py --n-ens 40 --base-path /lustre/scratch/tsupinie/05June2009/ --job-name 1kmf-my2 --n-cores 960 --mpi-config 2 12 --ens-start 10800 --ens-end 14400 --ens-step 300 --assim-step 300 --initial-conditions /lustre/scratch/tsupinie/05June2009/3km-ensemble-tar/3kmf-control/ --subset-ic --covariance-inflation 0:mult=1.03 5400:mult=1.20,adapt=0.90 --piecewise --split-files --fcst-req 0:30 --init-fcst-req 0:50 --debug --restart

# Restart a run
#python run_experiment.py --n-ens 40 --base-path /lustre/scratch/tsupinie/05June2009/ --job-name 1kmf-ebr-no-mm-05XP-snd --n-cores 960 --mpi-config 2 12 --ens-start 10800 --ens-end 14400 --ens-step 300 --assim-step 300 --covariance-inflation 0:mult=1.03 5400:mult=1.20,adapt=0.90 --piecewise --split-files --restart --debug

# Do the ensemble forecast
#python run_experiment.py --n-ens  40 --base-path /lustre/scratch/tsupinie/05June2009/ --job-name 1kmf-my2 --n-cores 960 --mpi-config 2 12 --ens-start 14400 --ens-end 18000 --ens-step 300 --assim-step 300 --piecewise --chunk-size 1800 --split-files --subset-ic --boundary-conditions /lustre/scratch/tsupinie/05June2009/3km-ensemble-tar/3kmf-control/ --restart --free-forecast --debug

# Fix shit when kraken fucks up ...
#python run_experiment.py --n-ens 40 --base-path /lustre/scratch/tsupinie/05June2009/ --job-name 1kmf-my2 --n-cores 960 --mpi-config 2 12 --ens-start 15900 --ens-end 18000 --ens-step 300 --assim-step 300 --split-files --split-init auto --piecewise --chunk-size 2100 --free-forecast --free-fcst-req 2:30 --restart --debug

