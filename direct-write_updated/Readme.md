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

**Performance Testing**

TRIAL 1:
  ![e65cbf65-893f-4ad3-8c60-b4fa6c65f0b8](https://github.com/user-attachments/assets/4f6aa1de-0792-430d-aa44-ab54e9cf1c12)

TRIAL 2:
![04f2da51-7c7c-45be-b573-a5d2aecc88da](https://github.com/user-attachments/assets/7febc266-3c89-4de2-838b-97d7c92d0b76)

TRIAL 3:
![a9bf623b-45b2-4a37-b419-f230eace46f1](https://github.com/user-attachments/assets/fef0379d-b9af-4995-aec9-3607504f8276)




