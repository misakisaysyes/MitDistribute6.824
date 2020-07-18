package mr

// master.go调用该函数将一个作业分成多个map子任务
func split(files *[]string) []string {
	return *files
}