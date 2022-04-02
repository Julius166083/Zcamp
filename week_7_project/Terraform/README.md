## GCP DETAILS & SETUP:
Log into google cloud paltform (GCP) using your gmail if u have one, if you don't have one, first create it, summary of steps and links is shown bellow;
  * create gmail accountusing this link.
  * Open up google cloud platform (GCP) using this link.
  * Use your google account crdentials to log in on the prompts asking for your e-mail & password,
  * The GCP window opens up, using either that search window or the top right navigation tab search for project or 
    navigate to project creation.
  * Created a GCP project name my case its 'data-zoomcamp-338514 and select other suitable details that you need to 
    have.
  * setup a service account and authentication for the project, granted viewer role, IAM roles for the created service 
    account and enabled API for my project, this included;
  * Navigate to 'keys'tab and create a key.json file which there after creation, it downloads automatically. 
  * Rename the key to googl's default name (application_default_credentials.json) and stored it in a path directory that 
    will be shown upon execution of the following commands, you will need to creat file path your self. for this mine 
    was;

         C:/Users/Dell/AppData/Roaming/gcloud/application_default_credentials.json

  * Then execute google key.json authentication commands shown bellow, depending file directory path you created, mine
    were as follows;
    
              # Set environment variable to point to your downloaded GCP keys
       $ export GOOGLE_APPLICATION_CREDENTIALS=/Users/Dell/AppData/Roaming/gcloud/application_default_credentials.json

              # Refresh token/session, and verify authentication
       $ gcloud auth application-default login
       
  * Download and install google SDK on either your local desktop or ssh into google virtual machine and do the 
    installation to enable you interract with GCP resources.
  * Finally to create a google cloud bucket storage we shall use Terraform infrustructure as described bellow.

## TERRAFORM SETUP (IaS: Infrastructure as a Service)

  * Install terraform application that suits your operating system, for linux ubuntu,use bellow 
    link: 'https://www.terraform.io/downloads'
  * select terraform version 1.1.5 or latest, copy link address of "Amd64" depending on your operating system for linux,     which i used, link:'https://releases.hashicorp.com/terraform/1.1.4/terraform_1.1.5_linux_amd64.zip'

  * download using 'wget' command for linux python environment
  * 
         $ wget (https://releases.hashicorp.com/terraform/1.1.4/terraform_1.1.5_linux_amd64.zip

  * cd into directory path you've downloaded the zipped file, my case was "/home/Dell/bin" directory
  * To unzip the file use commands
  
         $unzip terraform_1.1.4_linux_amd64.zip
         
  * If command fails then first install the missing "unzip" module then try 'unzip' download command again.
  
         $ sudo apt-get install unzip
         
  * After installation, remove the zipped file with command, the extracted file which is also executable will be seen in 
    green color for linux systems.
    
         $ rm terraform_1.1.4_linux_amd64.zip
         
   * While still in that directory path on the terminal, for my case in "/home/Dell/bin"  directory do terraform 
     commands to assertain version install to be sure.
     
         $ terraform --version
         
   * Fine tune terraform files of 'main.tf' and 'variables.tf', edit them as shown in my sample terraform files,
   * execute a terraform init, plan, apply to initialize terraform and call its executable files as show bellow to
     go ahead set up enviroment, authenticate with google, verify the changes with 'terraform plan' then when 
     satisfied you have say like these in my case for 'variable.tf' (to include google bucket 
     name='data_lake_week_7_project', bigquery name ='energy_consmuption') the right information, do 'terraform apply'
                            
                            $ terraform init
                            $ terraform plan
                            $ terraform apply

  * google vm instances, there is no need of 'gcloud auth application-default login', they automatically authenticates
  
   
