## GCP DETAILS & SETUP 
. Created a GCP project: data-zoomcamp-338514
. setup a service account and authentication for the project, granted viewer role,
  downloaded service account-key (application_default_credentials.json) and stored it in
  C:/Users/Dell/AppData/Roaming/gcloud/application_default_credentials.json
. downloaded and installed google SDK and installed on my google virtual machine after 
  'shh vm-machine-name' from my local machine.
. Set environment variable to point to your downloaded GCP keys:
  export GOOGLE_APPLICATION_CREDENTIALS=/Users/Dell/AppData/Roaming/gcloud/application_default_credentials.json

. Refresh token/session, and verify authentication
  gcloud auth application-default login
. set up IAM roles for service account and enabled API for my project
. created a google cloud storage as my DATA LAKE using Terraform

## TERRAFORM SETUP (IaS)
. installed terraform application for linux ubuntu, nagivate to link 
  'https://www.terraform.io/downloads'
. select terraform version 1.1.5 or latest, copy link address "Amd64" depending on 
  ur operating system for linux (https://releases.hashicorp.com/terraform/1.1.4/terraform_1.1.5_linux_amd64.zip)

- cd into "bin" directory
- unzip terraform_1.1.4_linux_amd64.zip ( to unzip terraform zipped folder)
- sudo apt-get install unzip
- rm terraform_1.1.4_linux_amd64.zip (now u dont need it, you remain with executable file "terraform" shown in green to indicate its executable)
-while in bin directory do (terraform --version)
-execute a terraform plan go back to terraform folder in week1
-before executing terraform, need ".json" files
(Credentials will still be generated to the default location:[C:\Users\Dell\AppData\Roaming\gcloud\application_default_credentials.json])
-cd /c/Users/Dell/AppData/Roaming/gcloud (on my local machine coz thats werr i have the file)

export GOOGLE_APPLICATION_CREDENTIALS=/Users/Dell/AppData/Roaming/gcloud/application_default_credentials.json

-since we are on google vm no need of 'gcloud auth application-default login' automatically authenticates
-edit the terraform files
-main.tf
-variable.tf (to include google bucket name='data_lake_week_7_project', bigquery name ='energy_consmuption')
-run terraform commands
* terraform init
* terraform plan
* terraform apply