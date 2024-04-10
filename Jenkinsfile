// Set up the libraries
@Library('socrata-pipeline-library')

import com.socrata.ReleaseMetadataService
def rmsSupportedEnvironment = com.socrata.ReleaseMetadataService.SupportedEnvironment

// set up service and project variables
String service_server = 'soql-server-pg'
String project_wd_server = 'soql-server-pg'
String service_secondary = 'secondary-watcher-pg'
String project_wd_secondary = 'store-pg'
boolean isPr = env.CHANGE_ID != null
String lastStage

// instanciate libraries
def sbtbuild = new com.socrata.SBTBuild(steps, service_server, '.', [project_wd_server, project_wd_secondary])
def dockerize_server = new com.socrata.Dockerize(steps, service_server, BUILD_NUMBER)
def dockerize_secondary = new com.socrata.Dockerize(steps, service_secondary, BUILD_NUMBER)
def releaseTag = new com.socrata.ReleaseTag(steps, service_server)

pipeline {
  options {
    ansiColor('xterm')
    buildDiscarder(logRotator(numToKeepStr: '50'))
    disableConcurrentBuilds(abortPrevious: true)
    timeout(time: 100, unit: 'MINUTES')
  }
  parameters {
    string(name: 'AGENT', defaultValue: 'build-worker', description: 'Which build agent to use?')
    string(name: 'BRANCH_SPECIFIER', defaultValue: 'origin/main', description: 'Use this branch for building the artifact.')
    booleanParam(name: 'RELEASE_BUILD', defaultValue: false, description: 'Are we building a release candidate?')
    booleanParam(name: 'RELEASE_DRY_RUN', defaultValue: false, description: 'To test out the release build without creating a new tag.')
    string(name: 'RELEASE_NAME', defaultValue: '', description: 'For release builds, the release name which is used for the git tag and the deploy tag.')
  }
  agent {
    label params.AGENT
  }
  environment {
    DEPLOY_PATTERN = "${service_server}*"
    SECONDARY_DEPLOY_PATTERN = "${service_secondary}*"
    WEBHOOK_ID = 'WEBHOOK_IQ'
  }
  stages {
    stage('Build') {
      steps {
        script {
          lastStage = env.STAGE_NAME
          echo "Building sbt project..."
          sbtbuild.setScalaVersion("2.12")
          // This build is a little unusual; it actually has _two_ artifacts, this one
          // and one for store-pg-assembly.  sbtbuild isn't really set up to override names
          // for two separate artifacts, so here we override the primary, and we'll override
          // the secondary down below when it's passed to dockerize.
          sbtbuild.setSrcJar("soql-server-pg/target/soql-server-pg-assembly.jar")
          env.STORE_PG_ARTIFACT = "store-pg/target/store-pg-assembly.jar"
          sbtbuild.build()
        }
      }
    }
    stage('Docker Build:') {
      when {
        not { expression { isPr } }
      }
      parallel {
        stage ('Docker Build - SoQL Server') {
          steps {
            script {
              lastStage = env.STAGE_NAME
              if (params.RELEASE_BUILD) {
                env.DOCKER_TAG = dockerize_server.dockerBuildWithSpecificTag(
                  tag: params.RELEASE_NAME,
                  path: sbtbuild.getDockerPath(project_wd_server),
                  artifacts: [sbtbuild.getDockerArtifact(project_wd_server)]
                )
              } else {
                env.DOCKER_TAG = dockerize_server.dockerBuildWithDefaultTag(
                  version: 'STAGING',
                  sha: env.GIT_COMMIT,
                  path: sbtbuild.getDockerPath(project_wd_server),
                  artifacts: [sbtbuild.getDockerArtifact(project_wd_server)]
                )
              }
            }
          }
        }
        stage ('Docker Build - Secondary') {
          steps {
            script {
              lastStage = env.STAGE_NAME
              // Here's where we're getting the secondary artifact (named
              // via env.STORE_PG_ARTIFACT) out for dockerizing, per the
              // comment above.
              if (params.RELEASE_BUILD) {
                env.SECONDARY_DOCKER_TAG = dockerize_secondary.dockerBuildWithSpecificTag(
                  tag: params.RELEASE_NAME,
                  path: sbtbuild.getDockerPath(project_wd_secondary),
                  artifacts: [env.STORE_PG_ARTIFACT]
                )
              } else {
                env.SECONDARY_DOCKER_TAG = dockerize_secondary.dockerBuildWithDefaultTag(
                  version: 'STAGING',
                  sha: env.GIT_COMMIT,
                  path: sbtbuild.getDockerPath(project_wd_secondary),
                  artifacts: [env.STORE_PG_ARTIFACT]
                )
              }
            }
          }
        }
      }
      post {
        success {
          script {
            if (params.RELEASE_BUILD) {
              env.GIT_TAG = releaseTag.getFormattedTag(params.RELEASE_NAME)
              if (releaseTag.doesReleaseTagExist(params.RELEASE_NAME)) {
                echo "REBUILD: Tag ${env.GIT_TAG} already exists"
                return
              }
              if (params.RELEASE_DRY_RUN) {
                echo "DRY RUN: Would have created ${env.GIT_TAG} and pushed it to the repo"
                currentBuild.description = "${service_server}:${params.RELEASE_NAME} and ${service_secondary}:${params.RELEASE_NAME} - DRY RUN"
                return
              }
              releaseTag.create(params.RELEASE_NAME)
            }
          }
        }
      }
    }
    stage('Publish:') {
      when {
        allOf {
          not { expression { isPr } }
          not { expression { return params.RELEASE_BUILD && params.RELEASE_DRY_RUN } }
        }
      }
      parallel {
        stage('Publish - SoQL Server') {
          steps {
            script {
              lastStage = env.STAGE_NAME
              if (params.RELEASE_BUILD) {
                env.BUILD_ID = dockerize_server.publish(sourceTag: env.DOCKER_TAG)
              } else {
                env.BUILD_ID = dockerize_server.publish(
                  sourceTag: env.DOCKER_TAG,
                  environments: ['internal']
                )
              }
            }
          }
          post {
            success {
              script {
                if (params.RELEASE_BUILD) {
                  Map buildInfo = [
                    "project_id": service_server,
                    "build_id": env.BUILD_ID,
                    "release_id": params.RELEASE_NAME,
                    "git_tag": env.GIT_TAG
                  ]
                  createBuild(
                    buildInfo,
                    rmsSupportedEnvironment.staging //production
                  )
                }
              }
            }
          }
        }
        stage('Publish - Secondary') {
          steps {
            script {
              lastStage = env.STAGE_NAME
              if (params.RELEASE_BUILD) {
                env.SECONDARY_BUILD_ID = dockerize_secondary.publish(sourceTag: env.SECONDARY_DOCKER_TAG)
              } else {
                env.SECONDARY_BUILD_ID = dockerize_secondary.publish(
                  sourceTag: env.SECONDARY_DOCKER_TAG,
                  environments: ['internal']
                )
              }
            }
          }
          post {
            success {
              script {
                if (params.RELEASE_BUILD) {
                  Map buildInfo = [
                    "project_id": service_secondary,
                    "build_id": env.SECONDARY_BUILD_ID,
                    "release_id": params.RELEASE_NAME,
                    "git_tag": env.GIT_TAG
                  ]
                  createBuild(
                    buildInfo,
                    rmsSupportedEnvironment.staging //production
                  )
                }
              }
            }
          }
        }
      }
      post {
        success {
          script {
            currentBuild.description = "${env.BUILD_ID} & ${env.SECONDARY_BUILD_ID}"
          }
        }
      }
    }
    stage('Deploy:') {
      when {
        allOf {
          not { expression { isPr } }
          not { expression { return params.RELEASE_BUILD} }
        }
      }
      stages {
        stage('Deploy - SoQL Server') {
          steps {
            script {
              lastStage = env.STAGE_NAME
              marathonDeploy(
                serviceName: env.DEPLOY_PATTERN,
                tag: env.BUILD_ID,
                environment: 'staging',
                waitTime: '60'
              )
            }
          }
        }
        stage('Deploy - Secondary') {
          steps {
            script {
              lastStage = env.STAGE_NAME
              marathonDeploy(
                serviceName: env.SECONDARY_DEPLOY_PATTERN,
                tag: env.SECONDARY_BUILD_ID,
                environment: 'staging'
              )
            }
          }
        }
      }
    }
  }
  post {
    failure {
      script {
        if (env.JOB_NAME.contains("${service_server}/main")) {
          teamsMessage(
            message: "Build [${currentBuild.fullDisplayName}](${env.BUILD_URL}) has failed in stage ${lastStage}",
            webhookCredentialID: WEBHOOK_ID
          )
        }
      }
    }
  }
}
