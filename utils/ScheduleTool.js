// 定时任务工具
const schedule = require('node-schedule')

let rule = new schedule.RecurrenceRule()

class ScheduleTool {
    // 每天0点执行
    static dayJob(time, handle) {
        rule.minute = time
        schedule.scheduleJob(rule, handle)
    }
}

module.exports = ScheduleTool