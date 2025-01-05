# Databricks notebook source
# MAGIC %md
# MAGIC # Reading Bronze data from Volume and writing them into bronze tables

# COMMAND ----------

default_env_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
catalog = default_env_catalog
prod_catalog = "bmk_prod"
env = catalog.split("_")[-1]

# COMMAND ----------

import requests
import json
from pathlib import Path
from datetime import datetime
import copy


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType, MapType
from datetime import datetime

# COMMAND ----------

bronze_volume = Path(f"/Volumes/{catalog}/bronze/konzertmeister_attendance/")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.bronze.konzertmeister_attendance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading the data from the Konzertmeister API and saving it to JSON

# COMMAND ----------

def get_km_auth_token():
    login_url = "https://rest.konzertmeister.app/api/v2/login"
    password = dbutils.secrets.get(scope="bmk-key-vault-scope", key="km-test-user-password-post")
    mail = dbutils.secrets.get(scope="bmk-key-vault-scope", key="km-test-user-mail")
    # Set up the headers with the Content-Type
    headers = {
        'Content-Type': 'application/json'
    }

    # Your payload for the POST request
    payload = {
        "mail": mail,
        "password": password,
        "locale": "en_US",  # Replace with the desired locale
        "timezone": "Europe/Berlin"  # Replace with the desired timezone
    }

    # Send a POST request to the login endpoint
    login_response = requests.post(login_url, json=payload, headers=headers)

    # Check if the login was successful
    if login_response.status_code == 200:
            # Print the response headers to inspect them
            # print("Response headers:", login_response.headers)

            # Extract the specific header value (e.g., 'X-AUTH-TOKEN') if it contains JSON data
            auth_token_header = login_response.headers.get('X-AUTH-TOKEN')
            if auth_token_header:
                print("Auth token retrieved")
    return auth_token_header


# COMMAND ----------

def get_km_history(
    start_date: str,
    end_date: str,
):
    # The URL to access the data
    url = "https://rest.konzertmeister.app/api/v2/att/matrix/history"

    token = get_km_auth_token()

    # Set up the headers with the Bearer token and Content-Type
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Your payload for the POST request
    payload = {
        "start": f"{start_date}T00:00:00+02:00",
        "end": f"{end_date}T23:59:59+01:00",
        "parentOrgId": 14981,
        "subOrgIds": None,
        "tagIds": None,
        "typIds": [1, 2, 3, 4, 5],
        "typesAndTagsWithAnd": True,
    }

    # Send a POST request to the URL with the headers
    response = requests.post(url, json=payload, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        try:
            # Attempt to parse the response as JSON
            data = response.json()
            print("retrieved data")
            return data
        except ValueError:
            print("Failed to parse response as JSON")
            print("Response text:", response.text)

# COMMAND ----------

def km_get_request(url: str):
    """
    Get data from a given URL using a GET request with authorization.

    Parameters
    ----------
    url : str
        The URL to send the GET request to.

    Returns
    -------
    dict
        The JSON response from the GET request if successful.

    Raises
    ------
    ValueError
        If the response cannot be parsed as JSON.
    """

    token = get_km_auth_token()

    # Set up the headers with the Bearer token and Content-Type
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # Send a GET request to the URL with the headers
    response = requests.get(url, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        try:
            # Attempt to parse the response as JSON
            data = response.json()
            print("retrieved data")
            return data
        except ValueError:
            print("Failed to parse response as JSON")
            print("Response text:", response.text)

# COMMAND ----------

today = datetime.now().strftime('%Y-%m-%d')

data = get_km_history (
    start_date="2020-07-01", # first possible start date, everything before is not properly working from the Konzertmeister website
    end_date=today,
)

# COMMAND ----------

file_path = bronze_volume / f"attendance_km-2020-07-01_to_{today}.json"

# COMMAND ----------

with open(file_path, 'w') as f:
    json.dump(data, f, indent=4)

# COMMAND ----------

get_urls = {
    "members": "https://rest.konzertmeister.app/api/v2/org/14981/members?includeSubOrgMemberships=true",
    "roles": "https://rest.konzertmeister.app/api/v2/org/14981/roles",
    "org_info": "https://rest.konzertmeister.app/api/v2/org/14981",
}

# COMMAND ----------



# COMMAND ----------

data_add = {}
for key, value in get_urls.items():
    bronze_volume_i = Path(f"/Volumes/{catalog}/bronze/konzertmeister_{key}/")
    # Create the volume if it does not exist
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.bronze.konzertmeister_{key}")
    response = km_get_request(url=value)
    data_add[key] = response
    file_path_data = bronze_volume_i / f"{key}_km_{today}.json"
    with open(file_path_data, 'w') as f:
        json.dump(response, f, indent=4)



# COMMAND ----------

# MAGIC %md
# MAGIC ## Orgs

# COMMAND ----------

table_name_orgs = f"{catalog}.bronze.km_orgs"
spark.sql(f"DROP TABLE IF EXISTS {table_name_orgs}")

# Define the schema
schema_orgs = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("parentName", StringType(), True),
    StructField("parentId", StringType(), True),
    StructField("typId", IntegerType(), True),
    StructField("liRoleId", IntegerType(), True),
    StructField("liPendingRequest", StringType(), True),
    StructField("numApprovedMembers", IntegerType(), True),
    StructField("leaderNames", StringType(), True),
    StructField("secretaryNames", StringType(), True),
    StructField("coLeaderNames", StringType(), True),
    StructField("secretaryCount", IntegerType(), True),
    StructField("leaderCount", IntegerType(), True),
    StructField("coLeaderCount", IntegerType(), True),
    StructField("token", StringType(), True),
    StructField("attendanceVisibleOnlyLeaders", StringType(), True),
    StructField("sort", IntegerType(), True),
    StructField("imageUrl", StringType(), True),
    StructField("adminKmUserId", StringType(), True),
    StructField("adminKmUserName", StringType(), True),
    StructField("address", StringType(), True),
    StructField("paymentPlan", StringType(), True),
    StructField("attendanceVisibility", StringType(), True),
    StructField("descriptionVisibility", StringType(), True),
    StructField("tags", StringType(), True),
    StructField("select", StringType(), True),
    StructField("showAllAppointmentsToMembers", StringType(), True),
    StructField("allowMemberSendMessage", StringType(), True),
    StructField("rootFolderId", IntegerType(), True),
    StructField("referralCode", StringType(), True),
    StructField("privateLinkURL", StringType(), True),
    StructField("publicSiteId", IntegerType(), True),
    StructField("attInvitedActive", StringType(), True),
    StructField("grouporg", StringType(), True),
    StructField("imageGenerated", StringType(), True),
    StructField("timezoneId", StringType(), True),
    # Add other fields as per your data
])

# Create DataFrame with the defined schema
df_orgs = spark.createDataFrame(data['orgs'], schema_orgs)

# Write DataFrame to Delta table
df_orgs.write.format("delta").mode("overwrite").saveAsTable(table_name_orgs)

# COMMAND ----------

# MAGIC %md
# MAGIC # Appointments

# COMMAND ----------

table_name_appointments = f"{catalog}.bronze.km_appointments"
spark.sql(f"DROP TABLE IF EXISTS {table_name_appointments}")

# Define the schema
schema_appointments = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("creatorName", StringType(), True),
    StructField("description", StringType(), True),
    StructField("leaderOnlyDescription", StringType(), True),
    StructField("start", TimestampType(), True),
    StructField("end", TimestampType(), True),
    StructField("org", StringType(), True),
    StructField("typId", IntegerType(), True),
    StructField("active", BooleanType(), True),
    StructField("statusDeadline", TimestampType(), True),
    StructField("remindDeadline", TimestampType(), True),
    StructField("createdAt", TimestampType(), True),
    StructField("timeUndefined", BooleanType(), True),
    StructField("informCreatorOnAttendanceUpdate", StringType(), True),
    StructField("liEditAllowed", StringType(), True),
    StructField("liSecretaryAllowed", StringType(), True),
    StructField("liSharingAllowed", StringType(), True),
    StructField("liWritePinnwallAllowed", StringType(), True),
    StructField("liAttendance", StringType(), True),
    StructField("statistics", StringType(), True),
    StructField("allowOptionMaybe", StringType(), True),
    StructField("forceDescriptionOnNegativeReply", StringType(), True),
    StructField("location", StringType(), True),
    StructField("meetingPoint", StringType(), True),
    StructField("attendanceVisibility", StringType(), True),
    StructField("descriptionVisibility", StringType(), True),
    StructField("group", StringType(), True),
    StructField("parentImageUrl", StringType(), True),
    StructField("imageUrl", StringType(), True),
    StructField("qrCodeAttendRealUrl", StringType(), True),
    StructField("attendanceLimit", IntegerType(), True),
    StructField("positiveReplies", IntegerType(), True),
    StructField("numPinnwall", IntegerType(), True),
    StructField("numFiles", IntegerType(), True),
    StructField("numFeedFiles", IntegerType(), True),
    StructField("tags", StringType(), True),
    StructField("cancelDescription", StringType(), True),
    StructField("publicSharingUrl", StringType(), True),
    StructField("checkinQrCodeImageUrl", StringType(), True),
    StructField("attachFilesRoleId", IntegerType(), True),
    StructField("privateLinkURL", StringType(), True),
    StructField("publicsite", BooleanType(), True),
    StructField("distance", StringType(), True),
    StructField("attendanceUpdateConfig", StringType(), True),
    StructField("room", StringType(), True),
    StructField("playlist", StringType(), True),
    StructField("timezoneId", StringType(), True),
    # Add other fields as per your data
])

# Convert the data to the correct format
for appointment in data['appointments']:
    for field in ['start', 'end', 'statusDeadline', 'remindDeadline', 'createdAt']:
        if appointment[field] is not None:
            appointment[field] = datetime.strptime(appointment[field], '%Y-%m-%dT%H:%M:%SZ')

# Create DataFrame with the defined schema
df_appointments = spark.createDataFrame(data['appointments'], schema_appointments)

# Write DataFrame to Delta table
df_appointments.write.format("delta").mode("overwrite").saveAsTable(table_name_appointments)

# COMMAND ----------

# MAGIC %md
# MAGIC # kmUsers

# COMMAND ----------

table_name_kmUsers = f"{catalog}.bronze.km_kmUsers"
spark.sql(f"DROP TABLE IF EXISTS {table_name_kmUsers}")

# Define the schema
schema_kmUsers = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("firstname", StringType(), True),
        StructField("lastname", StringType(), True),
        StructField("userDisplay", StringType(), True),
        StructField("userSort", StringType(), True),
        StructField("birthday", TimestampType(), True),
        StructField("imageUrl", StringType(), True),
        StructField("imageGenerated", BooleanType(), True),
    ]
)

# Convert the data to the correct format
for user in data["kmUsers"]:
    for field in [
        # 'registered',
        "birthday",
    ]:
        if field not in user.keys():
            user[field] = None
        elif user[field] is not None and isinstance(user[field], str):
            user[field] = datetime.strptime(user[field], "%Y-%m-%dT%H:%M:%SZ")

# Create DataFrame with the defined schema
df_kmUsers = spark.createDataFrame(data["kmUsers"], schema_kmUsers)

# Write DataFrame to Delta table
df_kmUsers.write.format("delta").mode("overwrite").saveAsTable(table_name_kmUsers)

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS bronze.km_kmUsers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Konzertmeister Members with more info

# COMMAND ----------

table_name_members = f"{catalog}.bronze.km_members"
spark.sql(f"DROP TABLE IF EXISTS {table_name_members}")

# Define the schema
schema_members = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("firstname", StringType(), True),
        StructField("lastname", StringType(), True),
        StructField("mail", StringType(), True),
        StructField("mobilePhone", StringType(), True),
        StructField("registered", BooleanType(), True),
        StructField("mailEnabled", BooleanType(), True),
        StructField("mailPinnwallEnabled", BooleanType(), True),
        StructField("localAppReminderEnabled", BooleanType(), True),
        StructField("pushEnabled", BooleanType(), True),
        StructField("smsEnabled", BooleanType(), True),
        StructField("newsletterEnabled", BooleanType(), True),
        StructField("realMail", StringType(), True),
        StructField("mailVerified", BooleanType(), True),
        StructField("pending", BooleanType(), True),
        StructField("locale", StringType(), True),
        StructField("touAccepted", BooleanType(), True),
        StructField("trackingEnabled", BooleanType(), True),
        StructField("active", BooleanType(), True),
        StructField("tags", StringType(), True),
        StructField("userDisplay", StringType(), True),
        StructField("userSort", StringType(), True),
        StructField("representationParent", StringType(), True),
        StructField("representationChild", StringType(), True),
        StructField("allowGoogleServices", BooleanType(), True),
        StructField("allowAppleServices", BooleanType(), True),
        StructField("touAcceptedVersion", StringType(), True),
        StructField("autoAttendAppointmentOnCreate", BooleanType(), True),
        StructField("showAttendanceProfilePictures", BooleanType(), True),
        StructField("imageUrl", StringType(), True),
        StructField("imageGenerated", BooleanType(), True),
        # StructField("assignedSubOrgsAndGroups", MapType(StringType(), StringType()), True),
        StructField("assignedSubOrgsAndGroups", StringType(), True),
        StructField("showDashboardOnStart", BooleanType(), True),
    ]
)

# Convert the data to the correct format
data_members = copy.deepcopy(data_add["members"])
for user in data_members:
    user["assignedSubOrgsAndGroups"] = str(user["assignedSubOrgsAndGroups"])

# Create DataFrame with the defined schema
df_members = spark.createDataFrame(data_members, schema_members)

# Write DataFrame to Delta table
df_members.write.format("delta").mode("overwrite").saveAsTable(table_name_members)

# COMMAND ----------

# MAGIC %md
# MAGIC #matrix

# COMMAND ----------

table_name_matrix = f"{catalog}.bronze.km_matrix"
spark.sql(f"DROP TABLE IF EXISTS {table_name_matrix}")
# Define the schema
schema_matrix = StructType([
    StructField("id", IntegerType(), True),
    StructField("kmUserId", IntegerType(), True),
    StructField("appointmentId", IntegerType(), True),
    StructField("attending", BooleanType(), True),
    StructField("attendingReal", BooleanType(), True),
    StructField("description", StringType(), True),
    StructField("marked", BooleanType(), True),
    StructField("external", BooleanType(), True),
    StructField("updatedAt", TimestampType(), True),
    StructField("negative", BooleanType(), True),
    StructField("positive", BooleanType(), True),
    StructField("maybe", BooleanType(), True),
    StructField("unanswered", BooleanType(), True),
    # Add other fields as per your data
])

flattened_matrix = []

for _, elem in data['matrix'].items():
    flattened_matrix += elem

# Convert the data to the correct format
for user in flattened_matrix:
    for field in ['updatedAt']:
        if user[field] is not None:
            user[field] = datetime.strptime(user[field], '%Y-%m-%dT%H:%M:%SZ')


# Create DataFrame with the defined schema
df_matrix = spark.createDataFrame(flattened_matrix, schema_matrix)

# Write DataFrame to Delta table
df_matrix.write.format("delta").mode("overwrite").saveAsTable(table_name_matrix)

# COMMAND ----------

# MAGIC %md
# MAGIC # orgUserMapping

# COMMAND ----------

table_name_orgUserMapping = f"{catalog}.bronze.km_orgUserMapping"
spark.sql(f"DROP TABLE IF EXISTS {table_name_orgUserMapping}")

# Define the schema
schema_orgUserMapping = StructType([
    StructField("orgId", IntegerType(), True),
    StructField("kmUserId", IntegerType(), True),
    # Add other fields as per your data
])

mapped_orgUserMapping = []

for key, elem in data['orgUserMapping'].items():
    for value in elem:
        elem_dict = {"orgId": int(key), "kmUserId": int(value)}
        mapped_orgUserMapping.append(elem_dict)

# Create DataFrame with the defined schema
df_orgUserMapping = spark.createDataFrame(mapped_orgUserMapping, schema_orgUserMapping)

# Write DataFrame to Delta table
df_orgUserMapping.write.format("delta").mode("overwrite").saveAsTable(table_name_orgUserMapping)

# COMMAND ----------

# MAGIC %md
# MAGIC # kmUserInactiveOrgs

# COMMAND ----------

# MAGIC %md
# MAGIC seems to be empty - skipping

# COMMAND ----------

# MAGIC %md
# MAGIC # kmUserInvitedOrgs

# COMMAND ----------

table_name_kmUserInvitedOrgs = f"{catalog}.bronze.km_kmUserInvitedOrgs"
spark.sql(f"DROP TABLE IF EXISTS {table_name_kmUserInvitedOrgs}")

# Define the schema
schema_kmUserInvitedOrgs = StructType([
    StructField("kmUserId", IntegerType(), True),
    StructField("orgId", IntegerType(), True),
    StructField("appointmentId", IntegerType(), True),
    # Add other fields as per your data
])

mapped_kmUserInvitedOrgs = []

for kmUserId, elem in data['kmUserInvitedOrgs'].items():
    for value in elem:
        elem_dict = {"kmUserId": int(kmUserId), "orgId": int(value["orgId"]), "appointmentId": int(value["appointmentId"])}
        mapped_kmUserInvitedOrgs.append(elem_dict)

# Create DataFrame with the defined schema
df_kmUserInvitedOrgs = spark.createDataFrame(mapped_kmUserInvitedOrgs, schema_kmUserInvitedOrgs)

# Write DataFrame to Delta table
df_kmUserInvitedOrgs.write.format("delta").mode("overwrite").saveAsTable(table_name_kmUserInvitedOrgs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Roles

# COMMAND ----------

table_name_roles = f"{catalog}.bronze.km_roles"
spark.sql(f"DROP TABLE IF EXISTS {table_name_roles}")

# Define the schema
schema_roles = StructType(
    [
        StructField("kmUserId", IntegerType(), True),
        StructField("role", StringType(), True),
    ]
)

data_roles = copy.deepcopy(data_add["roles"])
flattened_roles = []

for key, elem in data_roles.items():
    for value in elem:
        elem_dict = {
            "kmUserId": int(value["id"]),
            "role": key,
        }
        flattened_roles.append(elem_dict)

# Create DataFrame with the defined schema
df_roles = spark.createDataFrame(flattened_roles, schema_roles)

# Write DataFrame to Delta table
df_roles.write.format("delta").mode("overwrite").saveAsTable(table_name_roles)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


