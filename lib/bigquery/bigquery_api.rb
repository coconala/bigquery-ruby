require 'google/api_client'
require 'google/api_client/auth/installed_app'
require 'json'

class BigQuery
  class BigQueryApi
    AUTH_SCOPE = 'https://www.googleapis.com/auth/bigquery'
    MAX_RESULTS = 10000

    class << self
      def auth_account_by_cert(auth_email, auth_key, auth_pass)
        key = Google::APIClient::KeyUtils.load_from_pkcs12(auth_key, auth_pass)

        asserter = Google::APIClient::JWTAsserter.new(auth_email, AUTH_SCOPE, key)
        asserter.authorize
      end

      def auth_installed_app(client_id, client_secret)
        params = {
          :client_id => client_id,
          :client_secret => client_secret,
          :scope => AUTH_SCOPE,
        }

        flow = Google::APIClient::InstalledAppFlow.new(params)
        flow.authorize
      end
    end


    def initialize(auth)
      @client = Google::APIClient.new(application_name: NAME, application_version: VERSION)
      @client.authorization = auth

      @bq = @client.discovered_api('bigquery', 'v2')
    end


    def projects_list
      execute(@bq.projects.list)
    end


    def datasets_list(project_id)
      execute(@bq.datasets.list, params: { projectId: project_id })
    end

    def datasets_get(dataset_id)
      execute(@bq.datasets.get, params: { projectId: project_id, datasetId: dataset_id })
    end


    def tables_list(project_id, dataset_id)
      execute(@bq.tables.list, params: { projectId: project_id, datasetId: dataset_id })
    end

    def tables_insert(project_id, dataset_id, table_id, schema)
      body = {
        tableReference: {
          "projectId": project_id,
          "datasetId": dataset_id,
          "tableId": table_id,
        },
        schema: { fields: schema },
      }

      execute(@bq.tables.insert, params: { projectId: project_id, datasetId: dataset_id }, body: body)
    end


    def jobs_list(project_id)
      execute(@bq.jobs.list, params: { projectId: project_id })
    end

    def jobs_get(project_id, job_id)
      execute(@bq.jobs.get, params: { projectId: project_id, jobId: job_id })
    end

    def jobs_query(project_id, dataset_id, sql, dry_run = false)
      body = {
        query: sql,
        defaultDataset: { datasetId: dataset_id },
        maxResults: MAX_RESULTS,
        timeoutMs: 10000,
        dryRun: dry_run,
      }

      execute(@bq.jobs.query, params: { projectId: project_id }, body: body)
    end

    def jobs_insert(project_id, dataset_id, sql, dry_run = false)
      body = {
        configuration: {
          query: {
            query: sql,
            defaultDataset: { datasetId: dataset_id },
            priority: 'INTERACTIVE',   # or 'BATCH'
            allowLargeResults: false,
            useQueryCache: true,
          },
          dryRun: dry_run,
        },
      }

      execute(@bq.jobs.insert, params: { projectId: project_id }, body: body)
    end

    def jobs_get_query_results(project_id, job_id)
      params = {
        projectId: project_id,
        jobId: job_id,
        maxResults: MAX_RESULTS,
        timeoutMs: 10000,
      }

      execute(@bq.jobs.get_query_results, params: params)
    end


    private
    def execute(method, params: nil, body: nil)
      args = { api_method: method }
      args[:parameters] = params if !params.nil?
      args[:body_object] = body if !body.nil?

      res = JSON.parse(@client.execute(args).response.body)
      raise BigQueryError.new(res['error']['message']) if res['error']
      res
    end
  end
end
