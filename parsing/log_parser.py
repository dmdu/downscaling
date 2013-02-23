import time

class CondorJob(object):

    def __init__(self):

        self.id = None
        self.submitted_time = None
        self.scheduled_time = None
        self.terminated_time = None

    def __str__(self):
        return "Job id is %s, was submitted on %s and scheduled on %s and terminated on %s" % (
            self.id, time.strftime("%m-%d %H:%M:%S", self.submitted_time) ,
            time.strftime("%m-%d %H:%M:%S",self.scheduled_time), time.strftime("%m-%d %H:%M:%S", self.terminated_time))


class CondorParser(object):

    def __init__(self, condor_logfile):
        self.condor_logfile = condor_logfile
        self.condor_jobs_db = {}

    def parse_line(self, line, line_type):
        line_as_array = line.strip().split()
        try:
            job_id = line_as_array[1].split("(")[1].split(")")[0]
            job_month = line_as_array[2].split("/")[0]
            job_day = line_as_array[2].split("/")[1]
            job_time = line_as_array[3]
            job_timestamp = time.strptime("2012-%s-%s %s" % (job_month, job_day, job_time),"%Y-%m-%d %H:%M:%S")
            condor_job = CondorJob()

            if line_type == "submitted":
                condor_job.submitted_time = job_timestamp
            elif line_type == "executing":
                condor_job.scheduled_time = job_timestamp
            elif line_type == "terminated":
                condor_job.terminated_time = job_timestamp
            condor_job.id = job_id
            return condor_job
        except :
            pass

    def store_job(self, condor_job):
        if not condor_job.id in self.condor_jobs_db:
            self.condor_jobs_db[condor_job.id] = condor_job

        stored_job = self.condor_jobs_db[condor_job.id]

        if not stored_job.submitted_time and condor_job.submitted_time:
            self.condor_jobs_db[stored_job.id].submitted_time = condor_job.submitted_time

        elif not stored_job.scheduled_time and condor_job.scheduled_time:
            self.condor_jobs_db[stored_job.id].scheduled_time = condor_job.scheduled_time

        elif not stored_job.terminated_time and condor_job.terminated_time:
            self.condor_jobs_db[stored_job.id].terminated_time = condor_job.terminated_time


    def parse_file(self):
        with open(self.condor_logfile) as file_obj:
            condor_job = None
            for log_line in file_obj:
                if log_line.find("Job submitted from") != -1:
                    condor_job = self.parse_line(log_line, "submitted")

                elif log_line.find("Job executing on") != -1:
                    condor_job = self.parse_line(log_line, "executing")

                elif log_line.find("Job terminated.") != -1:
                    condor_job = self.parse_line(log_line, "terminated")

                if condor_job:
                    self.store_job(condor_job)

    def show(self):
        for job_id, condor_job in self.condor_jobs_db.iteritems():
            print condor_job

    def create_submitfile(self, dest_file):
        template = "Universe = vanilla\nExecutable = sleep\nLog = sleep.log\nOutput = sleep.out\nError = sleep.error\n"
        with open(dest_file, 'w') as file_obj:
            file_obj.write(template)
            for condor_jobs in self.condor_jobs_db.values():
                elapsed_time = time.mktime(condor_jobs.terminated_time) - time.mktime(condor_jobs.scheduled_time)
                file_obj.write("Arguments = %d\nQueue\n" % (int(elapsed_time)))

    def create_submitfiles(self, dest_file_suffix):

        sorted_sub_time = sorted([ x.submitted_time for x in self.condor_jobs_db.values() ])

        template = "Universe = vanilla\nExecutable = sleep\nLog = sleep.log\nOutput = sleep.out\nError = sleep.error\n"

        last_job_index = len(sorted_sub_time) - 1

        for index in range(len(sorted_sub_time)):
            each = sorted_sub_time[index]
            for job_id, condor_job in self.condor_jobs_db.iteritems():
                if each == condor_job.submitted_time:
                    index_str = str(index)
                    file_name = "%s%s" % (index_str.zfill(5), dest_file_suffix)
                    with open(file_name, 'w') as file_obj:
                        file_obj.write(template)
                        elapsed_time = time.mktime(condor_job.terminated_time) - time.mktime(condor_job.scheduled_time)

                        #print "Index %d" % index
                        #print "Last job index %d" % last_job_index
                        if index == last_job_index:
                            #sleep_time = 3 * elapsed_time
                            sleep_time = 120
                        else:
                            next_submit_time = sorted_sub_time[index + 1]
                            submit_time = condor_job.submitted_time
                            sleep_time = time.mktime(next_submit_time) - time.mktime(submit_time)

                        file_obj.write("Arguments = %d\nQueue\n#SLEEP %d\n" % (int(elapsed_time), int(sleep_time)))
                    index += 1
                    print condor_job

# Usage Note :
log_file = "large.log"
cp = CondorParser(log_file)
cp.parse_file()
cp.show()
cp.create_submitfile("condor.submit")

# log_file = "gradual.log"
# cp = CondorParser(log_file)
# cp.parse_file()
# cp.show()
# cp.create_submitfiles(".submit")
