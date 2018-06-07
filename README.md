# project_monitor
监控系统，包含定时任务脚本等

1 0 8 * * 3 python /application/search/bin/find_error_task.py \n
2 0 0 2 * * python /application/search/bin/flow_control_copy/flow_control_copy.py \n
3 0 6 * * * python /application/search/urlImport.py \n
