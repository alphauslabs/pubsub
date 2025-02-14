#TASK:
**endpointURL = "http://<VM_IP>:PORT/write"**

**What's new as of 02/14/2025:**

  ✅ Updated test codes in the branch:**
      -Imported uuid package and implemented unique ID generation for messages.
      -Updated timestamp handling: time.Now().UTC().Truncate(time.Second)
      -Implemented logging for successful Spanner writes.

  ✅ Collaborated with the test publisher to run test codes inside VMs

  ✅ Set up test environments in VMs
      -Configured ports and credentials for running test cases.
      -Deployed and tested code on multiple VMs.

**Problems Encountered:**
  Resolved issues with conflicting Go versions on VMs.
    -system package manager installs incompatible go version by default.
    Fix: installed compatible go version via wget.

**Direct-Write Testing**

TRIAL 1:
  Total published messages: 10,000.
  Messages per second: 315.50
![image](https://github.com/user-attachments/assets/1ac3cc56-a519-48c6-a2a2-c8860f3ff152)


TRIAL 2:
  Messages per second: 290


