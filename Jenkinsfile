@Library('socrata-pipeline-library@7.0.0') _

List projects = [
    [
        compiled: true,
        deploymentEcosystem: 'marathon-mesos',
        marathonInstanceNamePattern: 'soql-server-pg*',
        name: 'soql-server-pg',
        paths: [
            dockerBuildContext: 'soql-server-pg/docker',
        ],
        type: 'service',
    ],
    [
        compiled: true,
        deploymentEcosystem: 'marathon-mesos',
        marathonInstanceNamePattern: 'secondary-watcher-pg*',
        name: 'secondary-watcher-pg',
        paths: [
            dockerBuildContext: 'store-pg/docker',
        ],
        type: 'service',
    ],
]

commonPipeline(
    defaultBuildWorker: 'build-worker-pg13',
    jobName: 'soql-postgres-adapter',
    language: 'scala',
    languageOptions: [
        isMultiProjectRepository: true,
    ],
    projects: projects,
    teamsChannelWebhookId: 'WORKFLOW_IQ',
    timeout: 100,
)
