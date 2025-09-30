@Library('socrata-pipeline-library@9.7.0')

import com.socrata.ReleaseMetadataService
def rmsSupportedEnvironment = com.socrata.ReleaseMetadataService.SupportedEnvironment

// set up service and project variables
String service_server = 'soql-server-pg'
String project_wd_server = 'soql-server-pg'
String service_secondary = 'secondary-watcher-pg'
String project_wd_secondary = 'store-pg'
boolean isPr = env.CHANGE_ID != null
boolean isHotfix = isHotfixBranch(env.BRANCH_NAME)
boolean skip = false
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
    string(name: 'AGENT', defaultValue: 'build-worker-pg13', description: 'Which build agent to use?')
    string(name: 'BRANCH_SPECIFIER', defaultValue: 'origin/main', description: 'Use this branch for building the artifact.')
    booleanParam(name: 'RELEASE_BUILD', defaultValue: false, description: 'Are we building a release candidate?')
    booleanParam(name: 'RELEASE_DRY_RUN', defaultValue: false, description: 'To test out the release build.')
    string(name: 'RELEASE_NAME', description: 'For release builds, the release name which is used in the git tag and the build id.')
  }
  agent {
    label params.AGENT
  }
  environment {
    DEPLOY_PATTERN = "${service_server}*"
    SECONDARY_DEPLOY_PATTERN = "${service_secondary}*"
    WEBHOOK_ID = 'WORKFLOW_EGRESS_AUTOMATION'
  }
  stages {
    stage('Hotfix') {
      when {
        expression { isHotfix }
      }
      steps {
        script {
          lastStage = env.STAGE_NAME
          if (releaseTag.noCommitsOnHotfixBranch(env.BRANCH_NAME)) {
            skip = true
            echo "SKIP: Skipping the rest of the build because there are no commits on the hotfix branch yet"
          } else {
            env.CURRENT_RELEASE_NAME = releaseTag.getReleaseName(env.BRANCH_NAME)
            env.HOTFIX_NAME = releaseTag.getHotfixName(env.CURRENT_RELEASE_NAME)
          }
        }
      }
    }
    stage('Generate Leaked Secrets Report') {
      when {
        not { expression { skip } }
      }
      steps {
        script {
          lastStage = env.STAGE_NAME
          assert isInstalled('gitleaks'): 'gitleaks is missing.'
          String secretsReportFileName = 'gitleaks-report.json'
          String gitleaksCommand = getGitleaksCommand secretsReportFileName
          assert sh (script: gitleaksCommand, returnStatus: true) == 0: \
              'Attempt to run gitleaks failed.'
          echo "Generated report ${secretsReportFileName}."
          archiveArtifacts artifacts: secretsReportFileName, fingerprint: true
        }
      }
    }
    stage('Build') {
      when {
        not { expression { skip } }
      }
      steps {
        script {
          lastStage = env.STAGE_NAME
          sbtbuild.setScalaVersion("2.12")
          // This build is a little unusual; it actually has _two_ artifacts, this one
          // and one for store-pg-assembly.  sbtbuild isn't really set up to override names
          // for two separate artifacts, so here we override the primary, and we'll override
          // the secondary down below when it's passed to docker.
          sbtbuild.setSrcJar("soql-server-pg/target/soql-server-pg-assembly.jar")
          env.STORE_PG_ARTIFACT = "store-pg/target/store-pg-assembly.jar"
          sbtbuild.build()
          // This needs to be set before the docker build stages since they run in parallel
          if (params.RELEASE_BUILD || isHotfix) {
            env.VERSION = (isHotfix) ? env.HOTFIX_NAME : params.RELEASE_NAME
          }
        }
      }
    }
    stage('Docker Build:') {
      when {
        allOf {
          not { expression { isPr } }
          not { expression { skip } }
        }
      }
      parallel {
        stage ('Docker Build - SoQL Server') {
          steps {
            script {
              lastStage = env.STAGE_NAME
              if (params.RELEASE_BUILD || isHotfix) {
                env.DOCKER_TAG = dockerize_server.dockerBuildWithSpecificTag(
                  tag: env.VERSION,
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
              if (params.RELEASE_BUILD || isHotfix) {
                env.SECONDARY_DOCKER_TAG = dockerize_secondary.dockerBuildWithSpecificTag(
                  tag: env.VERSION,
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
            if (isHotfix) {
              env.GIT_TAG = releaseTag.create(env.HOTFIX_NAME)
            } else if (params.RELEASE_BUILD) {
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
          not { expression { skip } }
          not { expression { return params.RELEASE_BUILD && params.RELEASE_DRY_RUN } }
        }
      }
      stages {
        stage('Publish - SoQL Server') {
          steps {
            script {
              lastStage = env.STAGE_NAME
              if (isHotfix || params.RELEASE_BUILD) {
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
                if (isHotfix || params.RELEASE_BUILD) {
                  // These environment variables apply for both services
                  // They cannot be run in parallel because in the post stages there are race conditions
                  env.PURPOSE = (isHotfix) ? 'hotfix' : 'initial'
                  env.RELEASE_ID = (isHotfix) ? env.CURRENT_RELEASE_NAME : params.RELEASE_NAME
                  Map buildInfo = [
                    "project_id": service_server,
                    "build_id": env.BUILD_ID,
                    "release_id": env.RELEASE_ID,
                    "git_tag": env.GIT_TAG,
                    "purpose": env.PURPOSE,
                  ]
                  createBuild(
                    buildInfo,
                    rmsSupportedEnvironment.production
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
              if (isHotfix || params.RELEASE_BUILD) {
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
                if (isHotfix || params.RELEASE_BUILD) {
                  Map buildInfo = [
                    "project_id": service_secondary,
                    "build_id": env.SECONDARY_BUILD_ID,
                    "release_id": env.RELEASE_ID,
                    "git_tag": env.GIT_TAG,
                    "purpose": env.PURPOSE,
                  ]
                  createBuild(
                    buildInfo,
                    rmsSupportedEnvironment.production
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
          not { expression { skip } }
          not { expression { return params.RELEASE_BUILD} }
        }
      }
      stages {
        stage('Deploy - SoQL Server') {
          steps {
            script {
              lastStage = env.STAGE_NAME
              env.ENVIRONMENT = (isHotfix) ? 'rc' : 'staging'
              marathonDeploy(
                serviceName: env.DEPLOY_PATTERN,
                tag: env.BUILD_ID,
                environment: env.ENVIRONMENT,
                waitTime: '60'
              )
              // While working on migrating from marathon to ECS, we are keeping the tagged images up to date
              // Once the migration is done, we will remove the marathonDeploy and leave in place this publish which triggers the ECS deployment
              env.TARGET_DEPLOY_TAG = (env.ENVIRONMENT == 'rc') ? 'rc' : 'latest'
              dockerize_server.publish(
                sourceTag: env.DOCKER_TAG,
                targetTag: env.TARGET_DEPLOY_TAG,
                environments: ['internal']
              )
            }
          }
          post {
            success {
              script {
                if (isHotfix) {
                  Map deployInfo = [
                    "build_id": env.BUILD_ID,
                    "environment": env.ENVIRONMENT,
                  ]
                  createDeployment(
                    deployInfo,
                    rmsSupportedEnvironment.production
                  )
                }
              }
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
                environment: env.ENVIRONMENT,
              )
              // While working on migrating from marathon to ECS, we are keeping the tagged images up to date
              // Once the migration is done, we will remove the marathonDeploy and leave in place this publish which triggers the ECS deployment
              dockerize_secondary.publish(
                sourceTag: env.SECONDARY_DOCKER_TAG,
                targetTag: env.TARGET_DEPLOY_TAG,
                environments: ['internal']
              )
            }
          }
          post {
            success {
              script {
                if (isHotfix) {
                  Map deployInfo = [
                    "build_id": env.SECONDARY_BUILD_ID,
                    "environment": env.ENVIRONMENT,
                  ]
                  createDeployment(
                    deployInfo,
                    rmsSupportedEnvironment.production
                  )
                }
              }
            }
          }
        }
      }
    }
  }
  post {
    failure {
      script {
        boolean buildingMain = (env.JOB_NAME.contains("${service_server}/main"))
        if (buildingMain) {
          teamsWorkflowMessage(
            message: "[${currentBuild.fullDisplayName}](${env.BUILD_URL}) has failed in stage ${lastStage}",
            workflowCredentialID: WEBHOOK_ID
          )
        }
      }
    }
  }
}
