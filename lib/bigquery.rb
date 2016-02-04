require 'bigquery/bigquery_api'
require 'bigquery/bigquery_error'
require 'bigquery/query_async'
require 'bigquery/query_result'
require 'bigquery/resource'
require 'bigquery/version'

class BigQuery
  DEFAULT_KEY_PASS = 'notasecret'

  attr_accessor :project_id, :dataset_id

  def initialize(project_id: nil, dataset_id: nil, api: nil,
                 auth_email: nil, auth_key: nil, auth_pass: DEFAULT_KEY_PASS,
                 client_id: nil, client_secret: nil)

    @project_id = project_id
    @dataset_id = dataset_id

    if !api.nil?
      @api = api
    else
      auth = do_auth(auth_email, auth_key, auth_pass, client_id, client_secret)
      @api = BigQueryApi.new(auth)
    end
  end


  def projects
    result = @api.projects_list
    result['projects'].map {|d| BigQuery::Resource.new(d) }
  end


  def datasets
    check_project!
    result = @api.datasets_list(@project_id)
    result['datasets'].map {|d| BigQuery::Resource.new(d) }
  end

  def jobs
    check_project!
    result = @api.jobs_list(@project_id)
    result['jobs'].map {|d| BigQuery::Resource.new(d) }
  end


  def tables
    check_dataset!
    result = @api.tables_list(@project_id, @dataset_id)
    result['tables'].map {|d| BigQuery::Resource.new(d) }
  end

  def create_table(table_id, schema)
    check_dataset!
    @api.tables_insert(@project_id, @dataset_id, table_id, schema)
  end

  def query(sql, dry_run: false)
    check_dataset!
    result = do_query(sql, dry_run)
    (dry_run) ? [] : BigQuery::QueryResult.new(result)
  end

  def query_async(sql, dry_run: false)
    check_dataset!
    result = @api.jobs_insert(@project_id, @dataset_id, sql, dry_run)
    job_id = result['jobReference']['jobId']
    (dry_run) ? nil : BigQuery::QueryAsync.new(@api, @project_id, job_id)
  end


  private
  def do_auth(auth_email, auth_key, auth_pass, client_id, client_secret)
    if !auth_email.nil?
      auth = BigQueryApi.auth_account_by_cert(auth_email, auth_key, auth_pass)
    end

    if !client_id.nil?
      auth = BigQueryApi.auth_installed_app(client_id, client_secret)
    end

    raise 'not authenticated' if auth.nil?
    auth
  end

  def do_query(sql, dry_run = false)
    job_id = nil
    res = @api.jobs_query(@project_id, @dataset_id, sql, dry_run)

    10.times do
      return res if res['jobComplete']

      job_id ||= res['jobReference']['jobId']
      res = @api.jobs_get_query_results(@project_id, job_id)
    end
    raise 'bigquery query failed'
  end

  def check_project!
    raise 'project is not selected. set project_id.' if @project_id.nil?
  end

  def check_dataset!
    check_project!
    raise 'dataset is not selected. set dataset_id.' if @dataset_id.nil?
  end
end
