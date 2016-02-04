class BigQuery
  class QueryAsync
    def initialize(api, project_id, job_id)
      @api = api
      @project_id = project_id
      @job_id = job_id
    end

    def done?
      result = @api.jobs_get(@project_id, @job_id)
      result['status']['state'] == 'DONE'
    end

    def job
      BigQuery::Resource.new(@api.jobs_get(@project_id, @job_id))
    end

    def result
      result = @api.jobs_get_query_results(@project_id, @job_id)
      BigQuery::QueryResult.new(result)
    end
  end
end
