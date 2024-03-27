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
boolean isHotfix = isHotfixBranch(env.BRANCH_NAME)
boolean isReleaseRebuild = false
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
  }
  parameters {
    string(name: 'AGENT', defaultValue: 'build-worker', description: 'Which build agent to use?')
    string(name: 'BRANCH_SPECIFIER', defaultValue: 'origin/main', description: 'Use this branch for building the artifact.')
    string(name: 'RELEASE_NAME', defaultValue: '', description: 'For release builds, the release name which is used for the git tag and the deploy tag.')
    booleanParam(name: 'RELEASE_BUILD', defaultValue: false, description: 'Are we building a release candidate?')
    booleanParam(name: 'RELEASE_DRY_RUN', defaultValue: false, description: 'To test out the release build without creating a new tag.')
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
    stage('Release Tag') {
      when {
        expression { return params.RELEASE_BUILD }
      }
      steps {
        script {
          lastStage = env.STAGE_NAME
          env.GIT_TAG = releaseTag.getFormattedTag(params.RELEASE_NAME)
          if (releaseTag.doesReleaseTagExist(params.RELEASE_NAME)) {
            isReleaseRebuild = true
            echo "REBUILD: Tag ${env.GIT_TAG} already exists -- checking out the tag"
            releaseTag.checkoutTag(params.RELEASE_NAME)
            return
          }
          if (params.RELEASE_DRY_RUN) {
            echo "DRY RUN: Would have created ${env.GIT_TAG} and pushed it to the repo"
            return
          }
          releaseTag.create(params.RELEASE_NAME)
        }
      }
    }
    stage('Hotfix Tag') {
      when {
        expression { isHotfix }
      }
      steps {
        script {
          lastStage = env.STAGE_NAME
          if (releaseTag.noCommitsOnHotfixBranch(env.BRANCH_NAME)) {
            skip = true
            echo "SKIP: Skipping the rest of the build because there are no commits on the hotfix branch yet"
            return
          }
          env.CURRENT_RELEASE_NAME = releaseTag.getReleaseName(env.BRANCH_NAME)
          env.HOTFIX_NAME = releaseTag.getHotfixName(env.CURRENT_RELEASE_NAME)
          env.GIT_TAG = releaseTag.create(env.HOTFIX_NAME)
        }
      }
    }
    stage('Build SoQL Server PG') {
      when {
        not { expression { skip } }
      }
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
    stage('Dockerize SoQL Server PG') {
      when {
        allOf {
          not { expression { isPr } }
          not { expression { skip } }
        }
      }
      steps {
        script {
          lastStage = env.STAGE_NAME
          if (params.RELEASE_BUILD || isHotfix) {
            env.REGISTRY_PUSH = (params.RELEASE_DRY_RUN) ? 'none' : 'all'
            env.VERSION = (isHotfix) ? env.HOTFIX_NAME : params.RELEASE_NAME
            env.DOCKER_TAG = dockerize_server.docker_build_specify_tag_and_push(env.VERSION, sbtbuild.getDockerPath(project_wd_server), sbtbuild.getDockerArtifact(project_wd_server), env.REGISTRY_PUSH)
          } else {
            env.REGISTRY_PUSH = 'internal'
            env.DOCKER_TAG = dockerize_server.docker_build('STAGING', env.GIT_COMMIT, sbtbuild.getDockerPath(project_wd_server), sbtbuild.getDockerArtifact(project_wd_server), env.REGISTRY_PUSH)
          }
        }
      }
      post {
        success {
          script {
            boolean requiresBuild = isHotfix || params.RELEASE_BUILD
            boolean buildBypassed = isReleaseRebuild || params.RELEASE_DRY_RUN
            if (requiresBuild && !buildBypassed) {
              env.PURPOSE = (isHotfix) ? 'hotfix' : 'initial'
              env.RELEASE_ID = (isHotfix) ? env.CURRENT_RELEASE_NAME : params.RELEASE_NAME
              Map buildInfo = [
                "project_id": service_server,
                "build_id": env.DOCKER_TAG,
                "release_id": env.RELEASE_ID,
                "git_tag": env.GIT_TAG,
                "purpose": env.PURPOSE,
              ]
              createBuild(
                buildInfo,
                rmsSupportedEnvironment.staging // production - for testing
              )
            }
          }
        }
      }
    }
    stage('Dockerize Secondary') {
      when {
        allOf {
          not { expression { isPr } }
          not { expression { skip } }
        }
      }
      steps {
        script {
          lastStage = env.STAGE_NAME
          // Here's where we're getting the secondary artifact (named
          // via env.STORE_PG_ARTIFACT) out for dockerizing, per the
          // comment above.
          if (params.RELEASE_BUILD || isHotfix) {
            env.SECONDARY_DOCKER_TAG = dockerize_secondary.docker_build_specify_tag_and_push(env.VERSION, sbtbuild.getDockerPath(project_wd_secondary), env.STORE_PG_ARTIFACT, env.REGISTRY_PUSH)
          } else {
            env.SECONDARY_DOCKER_TAG = dockerize_secondary.docker_build('STAGING', env.GIT_COMMIT, sbtbuild.getDockerPath(project_wd_secondary), env.STORE_PG_ARTIFACT, env.REGISTRY_PUSH)
          }
          currentBuild.description = (params.RELEASE_DRY_RUN) ? "${env.DOCKER_TAG} & ${env.SECONDARY_DOCKER_TAG} - DRY RUN" : "${env.DOCKER_TAG} & ${env.SECONDARY_DOCKER_TAG}"
        }
      }
      post {
        success {
          script {
            boolean requiresBuild = isHotfix || params.RELEASE_BUILD
            boolean buildBypassed = isReleaseRebuild || params.RELEASE_DRY_RUN
            if (requiresBuild && !buildBypassed) {
              Map buildInfo = [
                "project_id": service_secondary,
                "build_id": env.SECONDARY_DOCKER_TAG,
                "release_id": params.RELEASE_NAME,
                "git_tag": env.GIT_TAG,
                "purpose": env.PURPOSE,
              ]
              createBuild(
                buildInfo,
                rmsSupportedEnvironment.staging // production - for testing
              )
            }
          }
        }
      }
    }
    stage('Deploys:') {
      when {
        allOf {
          not { expression { isPr } }
          not { expression { skip } }
          not { expression { return params.RELEASE_BUILD} }
        }
      }
      stages {
        stage('Deploy SoQL Server PG') {
          steps {
            script {
              lastStage = env.STAGE_NAME
              env.ENVIRONMENT = (isHotfix) ? 'rc' : 'staging'
              marathonDeploy(serviceName: env.DEPLOY_PATTERN, tag: env.DOCKER_TAG, environment: env.ENVIRONMENT, waitTime: '60')
            }
          }
          post {
            success {
              script {
                if (isHotfix) {
                  Map deployInfo = [
                    "build_id": env.DOCKER_TAG,
                    "environment": env.ENVIRONMENT,
                  ]
                  createDeployment(
                    deployInfo,
                    rmsSupportedEnvironment.staging // production - for testing
                  )
                }
              }
            }
          }
        }
        stage('Deploy Secondary') {
          steps {
            script {
              lastStage = env.STAGE_NAME
              marathonDeploy(serviceName: env.SECONDARY_DEPLOY_PATTERN, tag: env.SECONDARY_DOCKER_TAG, environment: env.ENVIRONMENT)
            }
          }
          post {
            success {
              script {
                if (isHotfix) {
                  Map deployInfo = [
                    "build_id": env.SECONDARY_DOCKER_TAG,
                    "environment": env.ENVIRONMENT,
                  ]
                  createDeployment(
                    deployInfo,
                    rmsSupportedEnvironment.staging // production - for testing
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
        if (env.JOB_NAME.contains("${service_server}/main")) {
          teamsMessage(message: "Build [${currentBuild.fullDisplayName}](${env.BUILD_URL}) has failed in stage ${lastStage}", webhookCredentialID: WEBHOOK_ID)
        }
      }
    }
  }
}
