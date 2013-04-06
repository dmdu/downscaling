import time
import datetime
import sys
from lib.util import Command
import numpy as np

class CostCounter(object):

    def __init__(self, dir):

        self.directory = dir.rstrip('/')
        print "Directory: %s" % (self.directory)

    def convert_time(self, time_string, time_zone_conversion = False):
        # time_string's format: 09:52:24.968269
        hms = (time_string.split('.'))[0] #ignore microseconds
        hms_time = time.strptime(hms,'%H:%M:%S')
        hms_seconds = datetime.timedelta(hours=hms_time.tm_hour,minutes=hms_time.tm_min,seconds=hms_time.tm_sec).total_seconds()

        if time_zone_conversion:
            # Start time from sleep.log uses Chicago time; timestamps from node.log use Denver time
            # This conversion adds 1 hour to timestamps in Denver time
            hms_seconds += 3600

        return hms_seconds

    def obtain_start_time(self):

        file_name = "%s/%s" % (self.directory, "sleep.log")
        print "Sleep.log: %s" % (file_name)
        with open(file_name) as file_object:
            for line in file_object:
                if "submitted" in line:
                    items = line.split()
                    self.start = self.convert_time(items[3], False) #no time conversion; Chicago time
                    print "Start time: %s" % (self.start)
                    return

    def obtain_end_time(self):
        # cat /Users/dmdu/Dropbox/dmdu-Downscaling-Experiments/experiments/exprC/AP-Mix-Thresh-Infinite/20130224_215317/sleep.log | grep terminated | head -n 1

        file_name = "%s/%s" % (self.directory, "sleep.log")
        end_time_string = "cat %s | grep terminated | tail -n 1" % (file_name)
        #print "Command: %s " % (end_time_string)
        cmd = Command(end_time_string)
        code = cmd.execute()
        if code:
            print "Can't obtain the end time"
            print cmd.stderr
            self.end = None
        else:
            line = cmd.stdout
            items = line.split()
            self.end = self.convert_time(items[3], False) #no time conversion; Chicago time
            print "End time: %s" % (self.end)

    def obtain_termination_timestamps(self):

        file_name = "%s/%s" % (self.directory, "node.log")
        print "Node.log: %s" % (file_name)

        self.terminations = []

        with open(file_name) as file_object:
            for line in file_object:
                if "TERMINATED" in line:
                    items = line.split()
                    term = self.convert_time(items[1], True) # Convert to Chicago time
                    self.terminations.append(term)
        print "Termination timestamps: %s" % (str(self.terminations))

    def cost_of_one_instance_hour(self):

        # AWS pricing:
        #                   Linux/UNIX Usage	Windows Usage
        #Small (Default)	$0.060 per Hour	    $0.115 per Hour
        #Medium	            $0.120 per Hour	    $0.230 per Hour
        #Large	            $0.240 per Hour	    $0.460 per Hour
        #Extra Large	    $0.480 per Hour	    $0.920 per Hour

        return 0.060

    def cloud_UP_instance_hour_cost(self):

        # AWS pricing:
        #                   Linux/UNIX Usage	Windows Usage
        #Small (Default)	$0.060 per Hour	    $0.115 per Hour
        #Medium	            $0.120 per Hour	    $0.230 per Hour
        #Large	            $0.240 per Hour	    $0.460 per Hour
        #Extra Large	    $0.480 per Hour	    $0.920 per Hour

        return 0.060

    def cloud_DOWN_instance_hour_cost(self):

        # AWS pricing:
        #                   Linux/UNIX Usage	Windows Usage
        #Small (Default)	$0.060 per Hour	    $0.115 per Hour
        #Medium	            $0.120 per Hour	    $0.230 per Hour
        #Large	            $0.240 per Hour	    $0.460 per Hour
        #Extra Large	    $0.480 per Hour	    $0.920 per Hour

        return 0.240

    def obtain_cost_no_boundaries(self):
        # Does not consider hour boundaries

        instance_count = 16 # initially
        prev_timestamp = self.start

        total_cost = 0

        for term in self.terminations:
            diff = term - prev_timestamp

            cost = (diff/3600.0) * instance_count * self.cost_of_one_instance_hour()
            print "Cost += (%f/3600.0) * %d * %f     (i.e. +=%f)" % (diff, instance_count, self.cost_of_one_instance_hour(), cost)
            total_cost += cost

            instance_count -= 1
            prev_timestamp = term

        print "Total cost (without considering hour boundaries): %s" % (total_cost)

    def obtain_cost_with_boundaries_for_downscaling_in_free_cloud(self):
        # Consider hour boundaries
        # Calculate cost for an experiment when downscaling takes place in a non-free cloud and upscaling occurs in a free cloud

        instance_count = 16 # initially
        end_of_hour = self.start
        current_termination_index = 0
        hour_index = 1
        total_cost = 0
        end_flag = False

        while True:
            end_of_hour += 3600
            terminated_within_this_hour = 0
            while True:
                if current_termination_index < len(self.terminations):
                    if self.terminations[current_termination_index] < end_of_hour:
                        terminated_within_this_hour += 1
                        current_termination_index += 1
                    else:
                        cost = 1.0 * instance_count * self.cost_of_one_instance_hour()
                        print "Cost for hour %d = 1.0 * %d * %f     (i.e. %f)" % (hour_index, instance_count, self.cost_of_one_instance_hour(), cost)
                        total_cost += cost
                        hour_index += 1

                        instance_count -= terminated_within_this_hour
                        break # switch to the next hours
                else:
                    # No more terminations
                    cost = 1.0 * instance_count * self.cost_of_one_instance_hour()
                    print "Cost for the last hour = 1.0 * %d * %f     (i.e. %f)" % (instance_count, self.cost_of_one_instance_hour(), cost)
                    total_cost += cost
                    end_flag = True # Stop counting
                    break
            if end_flag:
                break

        print "Total cost (considering hour boundaries, downscaling in a non-free cloud): %s" % (total_cost)

    def obtain_cost_with_boundaries_both_clouds_nonfree(self):
        # Consider hour boundaries
        # Calculate cost for an experiment when both downscaling and upscaling take place in non-free clouds

        # initially
        cloud_UP_instance_count = 48
        cloud_DOWN_instance_count = 16

        end_of_hour = self.start
        current_termination_index = 0
        hour_index = 1
        total_cost = 0
        end_flag = False

        while True:
            end_of_hour += 3600
            terminated_within_this_hour = 0
            while True:
                if current_termination_index < len(self.terminations):

                    #print "Termination #%d, time: %f" % (current_termination_index, self.terminations[current_termination_index])

                    if self.terminations[current_termination_index] < end_of_hour:
                        terminated_within_this_hour += 1
                        current_termination_index += 1
                    else:
                        cost_UP = 1.0 * cloud_UP_instance_count * self.cloud_UP_instance_hour_cost()
                        print "UP   Cost for hour %d = 1.0 * %d * %f     (i.e. %f)" % (hour_index, cloud_UP_instance_count, self.cloud_UP_instance_hour_cost(), cost_UP)
                        cost_DOWN = 1.0 * cloud_DOWN_instance_count * self.cloud_DOWN_instance_hour_cost()
                        print "DOWN Cost for hour %d = 1.0 * %d * %f     (i.e. %f)" % (hour_index, cloud_DOWN_instance_count, self.cloud_DOWN_instance_hour_cost(), cost_DOWN)

                        total_cost += cost_UP + cost_DOWN
                        hour_index += 1

                        #instance_count -= terminated_within_this_hour
                        cloud_UP_instance_count += terminated_within_this_hour
                        cloud_DOWN_instance_count -= terminated_within_this_hour
                        break # switch to the next hours
                else:
                    # Handle last hour with terminations
                    cost_UP = 1.0 * cloud_UP_instance_count * self.cloud_UP_instance_hour_cost()
                    print "UP   Cost for hour %d = 1.0 * %d * %f     (i.e. %f)" % (hour_index, cloud_UP_instance_count, self.cloud_UP_instance_hour_cost(), cost_UP)
                    cost_DOWN = 1.0 * cloud_DOWN_instance_count * self.cloud_DOWN_instance_hour_cost()
                    print "DOWN Cost for hour %d = 1.0 * %d * %f     (i.e. %f)" % (hour_index, cloud_DOWN_instance_count, self.cloud_DOWN_instance_hour_cost(), cost_DOWN)
                    total_cost += cost_UP + cost_DOWN
                    cloud_UP_instance_count += terminated_within_this_hour
                    cloud_DOWN_instance_count -= terminated_within_this_hour

                    end_flag = True # Stop counting
                    break
            if end_flag:
                break

        # No more terminations; handle time after the last termination
        if self.end: # timestamp is found, not None
            hours_until_the_end = int(np.ceil((self.end - end_of_hour)/3600.0))
            if hours_until_the_end > 0:
                cost_UP = hours_until_the_end * cloud_UP_instance_count * self.cloud_UP_instance_hour_cost()
                print "UP   Remaining cost = %d * %d * %f     (i.e. %f)" % (hours_until_the_end, cloud_UP_instance_count, self.cloud_UP_instance_hour_cost(), cost_UP)
                cost_DOWN = hours_until_the_end * cloud_DOWN_instance_count * self.cloud_DOWN_instance_hour_cost()
                print "DOWN Remaining cost = %d * %d * %f     (i.e. %f)" % (hours_until_the_end, cloud_DOWN_instance_count, self.cloud_DOWN_instance_hour_cost(), cost_DOWN)
                total_cost += cost_UP + cost_DOWN

        print "Total cost (considering hour boundaries, both clouds are non-free): %s" % (total_cost)

        # For logging purposes:
        print "LOG", self.directory, total_cost

if __name__ == '__main__':
    counter = CostCounter(sys.argv[1])
    counter.obtain_start_time()
    counter.obtain_end_time()
    counter.obtain_termination_timestamps()
    #counter.obtain_cost_no_boundaries()

    #counter.obtain_cost_with_boundaries_for_downscaling_in_free_cloud()
    counter.obtain_cost_with_boundaries_both_clouds_nonfree()