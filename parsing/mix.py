import random

def parse_file(file_name, limit):
    if not limit:
        return None
    with open(file_name) as file_obj:
        count = 0
        jlist = []
        for log_line in file_obj:
            #print int(log_line)
            jlist.append(int(log_line))
            count += 1
            if count == limit:
                break
        return jlist

def mix(list1, list2):
    if list1:
        l1 = len(list1)
    else:
        l1 = 0
    if list2:
        l2 = len(list2)
    else:
        l2 = 0
    capacity = l1 + l2
    mixed = []
    #print "Capacity: %d" % capacity
    for i in range(capacity):
        if not list1:
            selection = 2
        elif not list2:
            selection = 1
        else:
            selection = random.randrange(0,2)
        element = None
        if selection == 1:
            element = list1[0]
            mixed.append(element)
            list1.remove(element)
        else:
            element = list2[0]
            mixed.append(element)
            list2.remove(element)
        #print "Selecton: %d" % selection
        #print "Element: %s" % element
    return mixed

f1 = "small.xls"
f2 = "large.xls"
out = "mix.xls"
f1_limit = 1000
f2_limit = 
l1 = parse_file(f1, f1_limit)
l2 = parse_file(f2, f2_limit)
m = mix(l1, l2)

template = "Universe = vanilla\nExecutable = sleep\nLog = sleep.log\nOutput = sleep.out\nError = sleep.error\n"
with open(out, 'w') as file_obj:
    file_obj.write(template)
    for elem in m:
        file_obj.write("Arguments = %d\nQueue\n" % (elem))