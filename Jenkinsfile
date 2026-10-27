// CI/CD for the CMA data pipelines.
//
//   Build & unit test ─► Local pipeline run + reconciliation notebook ─► DAG / SQL / Ansible checks
//     ─► Package & publish
//     ─► Deploy DEV  (auto, main)          ─► Smoke test DEV
//     ─► Deploy UAT  (approval)            ─► Smoke test UAT
//     ─► Deploy PROD (approval, tag only)  ─► Smoke test PROD (read-only)
//
// A failed deployment *or* a failed smoke test triggers an automatic Ansible rollback.

pipeline {
  agent { label 'maven-jdk17' }

  options {
    timestamps()
    timeout(time: 90, unit: 'MINUTES')
    disableConcurrentBuilds()
    buildDiscarder(logRotator(numToKeepStr: '30', artifactNumToKeepStr: '10'))
  }

  parameters {
    choice(name: 'TARGET', choices: ['both', 'hadoop', 'gcp'], description: 'Platform(s) to deploy')
    booleanParam(name: 'SKIP_DEPLOY', defaultValue: false, description: 'Build and test only')
    string(name: 'ROLLBACK_TO', defaultValue: '', description: 'Redeploy this existing version instead of building (e.g. 1.4.2)')
  }

  environment {
    MAVEN_OPTS        = '-Xmx2g'
    MVN               = 'mvn -B -ntp -s ci/maven-settings.xml'
    RELEASE_VERSION   = "${env.TAG_NAME ?: "1.0.${env.BUILD_NUMBER}-${env.GIT_COMMIT?.take(7)}"}"
    ANSIBLE_FORCE_COLOR = 'true'
  }

  stages {
    stage('Rollback') {
      when { expression { params.ROLLBACK_TO?.trim() } }
      steps {
        script {
          ['dev', 'uat', 'prod'].each { envName ->
            input message: "Roll ${envName} back to ${params.ROLLBACK_TO}?", submitter: 'cma-release-managers'
            deploy(envName, params.ROLLBACK_TO)
          }
          currentBuild.result = 'SUCCESS'
        }
      }
    }

    stage('Build & unit test') {
      when { expression { !params.ROLLBACK_TO?.trim() } }
      steps {
        sh "${MVN} versions:set -DnewVersion=${RELEASE_VERSION} -DgenerateBackupPoms=false"
        sh "${MVN} clean verify"
      }
      post {
        always {
          junit allowEmptyResults: false, testResults: '**/target/surefire-reports/*.xml'
        }
      }
    }

    stage('Static checks') {
      when { expression { !params.ROLLBACK_TO?.trim() } }
      parallel {
        stage('Airflow DAGs') {
          steps {
            sh '''
              python3 -m venv .venv && . .venv/bin/activate
              pip install -q -r airflow/requirements-dev.txt \
                --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt
              ruff check airflow
              CMA_ENV=dev pytest -q airflow/tests --junitxml=airflow-tests.xml
            '''
          }
          post { always { junit 'airflow-tests.xml' } }
        }
        stage('BigQuery SQL dry run') {
          steps {
            withCredentials([file(credentialsId: 'gcp-sa-cma-dev', variable: 'GOOGLE_APPLICATION_CREDENTIALS')]) {
              sh 'ci/bq_dry_run.sh cma-data-dev'
            }
          }
        }
        stage('Ansible') {
          steps {
            sh '''
              . .venv/bin/activate 2>/dev/null || python3 -m venv .venv && . .venv/bin/activate
              pip install -q ansible-core ansible-lint
              ansible-galaxy collection install -r ansible/requirements.yml
              ansible-lint ansible/
              for env in dev uat prod; do
                ansible-playbook -i ansible/inventories/$env ansible/playbooks/site.yml --syntax-check
              done
            '''
          }
        }
      }
    }

    stage('Package & publish') {
      when {
        allOf {
          expression { !params.ROLLBACK_TO?.trim() }
          anyOf { branch 'main'; buildingTag() }
        }
      }
      steps {
        withCredentials([usernamePassword(credentialsId: 'nexus-cma', usernameVariable: 'NEXUS_USER', passwordVariable: 'NEXUS_PASS')]) {
          sh "${MVN} deploy -DskipTests"
        }
        archiveArtifacts artifacts: '**/target/*-all.jar', fingerprint: true
      }
    }

    stage('Deploy DEV') {
      when {
        allOf {
          expression { !params.SKIP_DEPLOY && !params.ROLLBACK_TO?.trim() }
          anyOf { branch 'main'; buildingTag() }
        }
      }
      steps { script { deployWithRollback('dev') } }   // includes smoke test
    }

    stage('Deploy UAT') {
      when {
        allOf {
          expression { !params.SKIP_DEPLOY && !params.ROLLBACK_TO?.trim() }
          anyOf { branch 'main'; buildingTag() }
        }
      }
      steps {
        timeout(time: 2, unit: 'DAYS') {
          input message: "Deploy ${RELEASE_VERSION} to UAT?", submitter: 'cma-data-eng-leads'
        }
        script { deployWithRollback('uat') }
      }
    }

    stage('Deploy PROD') {
      when {
        allOf {
          expression { !params.SKIP_DEPLOY && !params.ROLLBACK_TO?.trim() }
          buildingTag()
        }
      }
      steps {
        timeout(time: 5, unit: 'DAYS') {
          input message: "Deploy ${RELEASE_VERSION} to PROD? (change ticket required)",
                parameters: [string(name: 'CHANGE_TICKET', description: 'Approved change record')],
                submitter: 'cma-release-managers'
        }
        script { deployWithRollback('prod') }
      }
    }
  }

  post {
    success {
      echo "Pipeline succeeded for ${RELEASE_VERSION}"
    }
    failure {
      emailext subject: "FAILED: ${env.JOB_NAME} #${env.BUILD_NUMBER} (${RELEASE_VERSION})",
               body: "See ${env.BUILD_URL}console",
               to: 'cma-data-eng@example.com'
    }
    always {
      cleanWs(deleteDirs: true, notFailBuild: true)
    }
  }
}

// ---------------------------------------------------------------- helpers

def deploy(String envName, String version) {
  withCredentials([
    sshUserPrivateKey(credentialsId: "hadoop-edge-${envName}", keyFileVariable: 'SSH_KEY', usernameVariable: 'SSH_USER'),
    file(credentialsId: "gcp-sa-cma-${envName}", variable: 'GOOGLE_APPLICATION_CREDENTIALS'),
    usernamePassword(credentialsId: 'nexus-cma', usernameVariable: 'NEXUS_USER', passwordVariable: 'NEXUS_PASS')
  ]) {
    sh """
      ansible-playbook -i ansible/inventories/${envName} ansible/playbooks/site.yml \
        --private-key "\$SSH_KEY" -u "\$SSH_USER" \
        -e release_version=${version} -e target=${params.TARGET}
    """
  }
}

def smokeTest(String envName) {
  withCredentials([
    sshUserPrivateKey(credentialsId: "hadoop-edge-${envName}", keyFileVariable: 'SSH_KEY', usernameVariable: 'SSH_USER'),
    file(credentialsId: "gcp-sa-cma-${envName}", variable: 'GOOGLE_APPLICATION_CREDENTIALS')
  ]) {
    // DEV/UAT: metadata, data-health and write-free Spark dry runs. PROD: read-only checks only.
    sh """
      ansible-playbook -i ansible/inventories/${envName} ansible/playbooks/smoke_test.yml \
        --private-key "\$SSH_KEY" -u "\$SSH_USER" \
        -e release_version=${env.RELEASE_VERSION} -e target=${params.TARGET}
    """
  }
}

def deployWithRollback(String envName) {
  try {
    stage("Deploy ${envName.toUpperCase()}: apply") { deploy(envName, env.RELEASE_VERSION) }
    stage("Smoke test ${envName.toUpperCase()}") { smokeTest(envName) }
  } catch (err) {
    echo "Deployment or smoke test in ${envName} failed: ${err}. Rolling back to the previous release."
    withCredentials([
      sshUserPrivateKey(credentialsId: "hadoop-edge-${envName}", keyFileVariable: 'SSH_KEY', usernameVariable: 'SSH_USER'),
      file(credentialsId: "gcp-sa-cma-${envName}", variable: 'GOOGLE_APPLICATION_CREDENTIALS')
    ]) {
      sh """
        ansible-playbook -i ansible/inventories/${envName} ansible/playbooks/rollback.yml \
          --private-key "\$SSH_KEY" -u "\$SSH_USER" -e target=${params.TARGET}
      """
    }
    error("Deployment to ${envName} failed and was rolled back: ${err}")
  }
}
