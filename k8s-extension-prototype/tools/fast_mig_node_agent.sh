#!/bin/sh
set -u

GPU_INDEX="${GPU_INDEX:-0}"

PROFILE_IDS_7="0"
PROFILE_IDS_4_3="5,9"
PROFILE_IDS_4_2_1="5,14,19"
PROFILE_IDS_4_1_1_1="5,19,19,19"
PROFILE_IDS_3_3="9,9"
PROFILE_IDS_3_2_1="9,14,19"
PROFILE_IDS_3_1_1_1="9,19,19,19"
PROFILE_IDS_3_2_2="9,14,14"
PROFILE_IDS_3_2_1_1="9,14,19,19"
PROFILE_IDS_3_1_1_1_1="9,19,19,19,19"
PROFILE_IDS_2_2_2_1="14,14,14,19"
PROFILE_IDS_2_2_1_1_1="14,14,19,19,19"
PROFILE_IDS_2_1_1_1_1_1="14,19,19,19,19,19"
PROFILE_IDS_1_1_1_1_1_1_1="19,19,19,19,19,19,19"

TEMPLATES="7 4+3 4+2+1 4+1+1+1 3+3 3+2+1 3+1+1+1 3+2+2 3+2+1+1 3+1+1+1+1 2+2+2+1 2+2+1+1+1 2+1+1+1+1+1 1+1+1+1+1+1+1"

usage() {
    cat <<'EOF'
fast_mig_node_agent.sh: minimal local MIG actuator for A100 40GB experiments

Usage:
  fast_mig_node_agent.sh list
  fast_mig_node_agent.sh clear [--gpu-index N]
  fast_mig_node_agent.sh apply TEMPLATE [--gpu-index N]
  fast_mig_node_agent.sh benchmark [--gpu-index N]

Templates:
  7
  4+3
  4+2+1
  4+1+1+1
  3+3
  3+2+1
  3+1+1+1
  3+2+2
  3+2+1+1
  3+1+1+1+1
  2+2+2+1
  2+2+1+1+1
  2+1+1+1+1+1
  1+1+1+1+1+1+1

Notes:
  - Run as root or inside a privileged NVIDIA container.
  - This bypasses GPU Operator MIG Manager and Kubernetes allocatable readiness.
  - Do not run while workloads are using the target GPU.
EOF
}

die() {
    echo "ERROR: $*" >&2
    exit 1
}

now_seconds() {
    awk '{print $1}' /proc/uptime
}

elapsed_seconds() {
    awk -v a="$1" -v b="$2" 'BEGIN { printf "%.3f", b - a }'
}

template_to_ids() {
    case "$1" in
        7) echo "$PROFILE_IDS_7" ;;
        4+3) echo "$PROFILE_IDS_4_3" ;;
        4+2+1) echo "$PROFILE_IDS_4_2_1" ;;
        4+1+1+1) echo "$PROFILE_IDS_4_1_1_1" ;;
        3+3) echo "$PROFILE_IDS_3_3" ;;
        3+2+1) echo "$PROFILE_IDS_3_2_1" ;;
        3+1+1+1) echo "$PROFILE_IDS_3_1_1_1" ;;
        3+2+2) echo "$PROFILE_IDS_3_2_2" ;;
        3+2+1+1) echo "$PROFILE_IDS_3_2_1_1" ;;
        3+1+1+1+1) echo "$PROFILE_IDS_3_1_1_1_1" ;;
        2+2+2+1) echo "$PROFILE_IDS_2_2_2_1" ;;
        2+2+1+1+1) echo "$PROFILE_IDS_2_2_1_1_1" ;;
        2+1+1+1+1+1) echo "$PROFILE_IDS_2_1_1_1_1_1" ;;
        1+1+1+1+1+1+1) echo "$PROFILE_IDS_1_1_1_1_1_1_1" ;;
        *) return 1 ;;
    esac
}

require_tools() {
    command -v nvidia-smi >/dev/null 2>&1 || die "nvidia-smi not found"
    command -v awk >/dev/null 2>&1 || die "awk not found"
    [ -r /proc/uptime ] || die "/proc/uptime is not readable"
}

clear_mig() {
    nvidia-smi mig -dci -i "$GPU_INDEX" >/dev/null 2>&1 || true
    nvidia-smi mig -dgi -i "$GPU_INDEX" >/dev/null 2>&1 || true
}

apply_template() {
    template="$1"
    ids="$(template_to_ids "$template")" || die "unknown template: $template"
    clear_mig
    start="$(now_seconds)"
    output="$(nvidia-smi mig -cgi "$ids" -C -i "$GPU_INDEX" 2>&1)"
    rc=$?
    end="$(now_seconds)"
    elapsed="$(elapsed_seconds "$start" "$end")"
    if [ "$rc" -ne 0 ]; then
        echo "$output" >&2
        echo "RESULT|apply|$template|$ids|$elapsed|CREATE_FAILED"
        exit "$rc"
    fi
    echo "$output"
    echo "RESULT|apply|$template|$ids|$elapsed|OK"
}

benchmark_template() {
    template="$1"
    ids="$(template_to_ids "$template")" || die "unknown template: $template"
    clear_mig
    create_start="$(now_seconds)"
    create_output="$(nvidia-smi mig -cgi "$ids" -C -i "$GPU_INDEX" 2>&1)"
    create_rc=$?
    create_end="$(now_seconds)"
    create_elapsed="$(elapsed_seconds "$create_start" "$create_end")"
    if [ "$create_rc" -ne 0 ]; then
        clear_mig
        echo "RESULT|benchmark|$template|$ids|$create_elapsed||CREATE_FAILED"
        echo "$create_output" | sed "s/^/DETAIL|$template|/"
        return
    fi

    delete_start="$(now_seconds)"
    nvidia-smi mig -dci -i "$GPU_INDEX" >/dev/null 2>&1
    nvidia-smi mig -dgi -i "$GPU_INDEX" >/dev/null 2>&1
    delete_rc=$?
    delete_end="$(now_seconds)"
    delete_elapsed="$(elapsed_seconds "$delete_start" "$delete_end")"
    if [ "$delete_rc" -eq 0 ]; then
        status="OK"
    else
        status="DELETE_FAILED"
        clear_mig
    fi
    echo "RESULT|benchmark|$template|$ids|$create_elapsed|$delete_elapsed|$status"
}

parse_gpu_index() {
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --gpu-index)
                shift
                [ "$#" -gt 0 ] || die "--gpu-index requires a value"
                GPU_INDEX="$1"
                ;;
            *)
                die "unknown argument: $1"
                ;;
        esac
        shift
    done
}

main() {
    [ "$#" -gt 0 ] || {
        usage
        exit 2
    }
    command="$1"
    shift
    require_tools
    case "$command" in
        list)
            nvidia-smi -L
            ;;
        clear)
            parse_gpu_index "$@"
            clear_mig
            nvidia-smi -L
            ;;
        apply)
            [ "$#" -gt 0 ] || die "apply requires TEMPLATE"
            template="$1"
            shift
            parse_gpu_index "$@"
            apply_template "$template"
            nvidia-smi -L
            ;;
        benchmark)
            parse_gpu_index "$@"
            for template in $TEMPLATES; do
                benchmark_template "$template"
            done
            clear_mig
            nvidia-smi -L
            ;;
        -h|--help|help)
            usage
            ;;
        *)
            die "unknown command: $command"
            ;;
    esac
}

main "$@"
