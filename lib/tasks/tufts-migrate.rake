require 'rake'

desc "Migrate all my objects"
namespace :tufts do

    dark_archive_users = [{ username: "mkorcy01", email: "Mike.Korcynski@tufts.edu", display_name: "Korcynski, Mike" },
                          { username: "bgoodm01", email: "brian.goodmon@tufts.edu", display_name: "Goodmon, Brian" },
                          { username: "batchuser", email: "batchuser@example.com", display_name: "batchuser" },
                          { username: "tlille01", email: "travis.lilleberg@tufts.edu", display_name: "Lilleberg, Travis" },
                          { username: "mpeach01", email: "margaret.peachy@tufts.edu", display_name: "Peachy, Margaret" },
                          { username: "apruit01", email: "adrienne.pruitt@tufts.edu", display_name: "Pruitt, Adrienne" },
                          { username: "dsanta02", email: "daniel.santamaria@tufts.edu", display_name: "Santamaria, Daniel" },
                          { username: "efranc02", email: "Elizabeth.Francis@tufts.edu", display_name: "Francis, Liz" },
                          { username: "smauro01", email: "sari.mauro@tufts.edu", display_name: "Mauro, Sari" }]
  desc "populate dark archives users"
  task populate_dark_archives_users: :environment do

    dark_archive_users.each do |dark_archive_user|
      admin_role = Role.find_by(name: 'admin')
      user = User.find_or_create_by!(username: dark_archive_user[:username], email: dark_archive_user[:email], display_name: dark_archive_user[:display_name]) do |u|
        u.password = SecureRandom.base64(24)
      end

      user.add_role('admin') unless user.roles.include? admin_role
    end
  end

  desc "populate mira users"
  task populate_mira_users: :environment do
    mira_users = [{ username: "amay02", email: "alexander.may@tufts.edu", display_name: "May, Alexander B." },
                          { username: "amorri06", email: "alicia.morris@tufts.edu", display_name: "Morris, Alicia M." }]

    mira_users = mira_users + dark_archive_users

    mira_users.each do |dark_archive_user|
      admin_role = Role.find_by(name: 'admin')
      user = User.find_or_create_by!(username: dark_archive_user[:username], email: dark_archive_user[:email], display_name: dark_archive_user[:display_name]) do |u|
        u.password = SecureRandom.base64(24)
      end

      user.add_role('admin') unless user.roles.include? admin_role
    end
  end
  desc "create migration user"
  task create_migration_user: :environment do
      @user = User.find_or_create_by!(email: 'f3migrationtool@tufts.edu') do |user|
        user.username = 'migration'
        user.password = SecureRandom.base64(24)
        user.add_role('admin')
      end
  end
  desc "define top level dca collections"
  task define_top_level_dca_collections: :environment do
    # top level collections
    collections = ['Manuscripts','New Nation Votes','Test Items','Tufts Scholarship','University Archives','University Publications']
      collections.each do |collection|
        if Collection.where(title: collection).empty?
          a = Collection.new(title: [collection])
          a.apply_depositor_metadata 'apruit01'
          a.visibility = Hydra::AccessControls::AccessRight::VISIBILITY_TEXT_VALUE_PUBLIC
          a.save!
        end
      end
  end

  desc "define top level tisch collections"
  task define_top_level_tisch_collections: :environment do
    # top level collections
    collections = ["Trove", "Student Scholarship", "Great Courses", "Faculty Scholarship", "Electronic Theses and Dissertations", "Digitized Books"]
      collections.each do |collection|
        if Collection.where(title: collection).empty?
          a = Collection.new(title: [collection])
          a.apply_depositor_metadata 'amay02'
          a.visibility = Hydra::AccessControls::AccessRight::VISIBILITY_TEXT_VALUE_PUBLIC
          a.save!
        end
      end
  end

  desc "update index"
  task update_index: :environment do
    spec = Gem::Specification.find_by_name("fedora-migrate")
    gem_root = spec.gem_dir
    row_count = 0

    CSV.foreach("#{gem_root}/voting_record_ids.csv", :headers => true, :header_converters => :symbol, encoding: "ISO8859-1:utf-8") do |row|
      pid = row[0]
      a = VotingRecord.find(pid)
      a.update_index
    end
  end

  desc "verify file_set counts"
  task verify_file_set_counts: :environment do
    spec = Gem::Specification.find_by_name("fedora-migrate")
    gem_root = spec.gem_dir
    row_count = 0
    file = File.open("/tmp/file_sets.txt", "w")

    CSV.foreach("#{gem_root}/voting_record_ids.csv", :headers => true, :header_converters => :symbol, encoding: "ISO8859-1:utf-8") do |row|
      pid = row[0]
  

      file.write "PID: #{pid}\n"
      a = VotingRecord.find(pid)
      count = a.file_sets.length
      file.write "#{pid}, #{count}\n"
      file.flush
    end
    file.close
  end

  desc "define secondary dca collections"
  task define_secondary_dca_collections: :environment do
    spec = Gem::Specification.find_by_name("fedora-migrate")
    gem_root = spec.gem_dir
    row_count = 0
    CSV.foreach("#{gem_root}/collection_f3_f4_mapping.csv", :headers => true, :header_converters => :symbol, :converters => :all, encoding: "ISO8859-1:utf-8") do |row|
      row_count = row_count + 1
      child_col = row[3]
      ead = row[2]
      ead = "tufts:UA069.DO.001.DO." + ead unless ead.blank?
      puts "#{row_count} row, collection: #{child_col} with ead #{ead}" unless child_col.blank?

      if Collection.where(title: child_col).empty?
        next if child_col.blank?
        secondary_col = Collection.new(title: [child_col])
        secondary_col.apply_depositor_metadata 'apruit01'
        secondary_col.ead = [ead]
        secondary_col.visibility = Hydra::AccessControls::AccessRight::VISIBILITY_TEXT_VALUE_PUBLIC
        secondary_col.save!

        primary_col = Collection.where(title: row[0])
        unless primary_col.empty?
          primary_col.first.add_members secondary_col.id
          primary_col.first.save!
        end
      end
    end
  end

  desc "delete and eradicate all collections"
  task eradicate_all_collections: :environment do
    Collection.all.each do |col| 
      id = col.id
      col.delete
      ActiveFedora::Base.eradicate(id)
    end
  end

  desc "delete all collections"
  task delete_all_collections: :environment do
    Collection.all.each {|col| col.delete }
  end

  desc "import dca collection mapping table used to add migrated records to proper collection"
  task create_dca_migration_mapping_table: :environment do
    spec = Gem::Specification.find_by_name("fedora-migrate")
    gem_root = spec.gem_dir

    db = SQLite3::Database.open "#{gem_root}/collections.sqlite3"
    db.execute("CREATE TABLE IF NOT EXISTS collection_map(collection VARCHAR(40) NOT NULL, pid VARCHAR(40) NOT NULL)")

    CSV.foreach("#{gem_root}/collection_f3_f4_mapping.csv", :headers => true, :header_converters => :symbol, :converters => :all, encoding: "ISO8859-1:utf-8") do |row|
      pid = row[5]
      puts "#{pid}"
      col = Collection.where(title: row[3])
      next if col.blank?
      db.execute "REPLACE INTO collection_map VALUES(\"#{col.first.id}\",\"#{pid}\")"
    end

    db.close if db
  end

  desc "import tisch collection mapping table used to add migrated records to proper collection"
  task create_tisch_migration_mapping_table: :environment do
    spec = Gem::Specification.find_by_name("fedora-migrate")
    gem_root = spec.gem_dir

    db = SQLite3::Database.open "#{gem_root}/collections.sqlite3"
    db.execute("CREATE TABLE IF NOT EXISTS collection_map(collection VARCHAR(40) NOT NULL, pid VARCHAR(40) NOT NULL)")

    CSV.foreach("#{gem_root}/Tisch_F4_Collections.csv", :headers => true, :header_converters => :symbol, :converters => :all, encoding: "ISO8859-1:utf-8") do |row|
      pid = row[0]
      puts "#{pid}"
      col = Collection.where(title: row[1])
      next if col.blank?
      db.execute "REPLACE INTO collection_map VALUES(\"#{col.first.id}\",\"#{pid}\")"
    end

    db.close if db
  end

  task migrate: :environment do
    results = FedoraMigrate.migrate_repository(namespace: "tufts", options: {})
    puts results
  end

   def convert_id(id)
     newid = id.downcase
     newid.slice!('tufts:')
     newid.tr!('.', '_')

     if number?(newid)
       newid =  "pid" + newid
     end

     newid
  end

  def number?(obj)
    obj = obj.to_s unless obj.is_a? String
    /\A[+-]?\d+(\.[\d]+)?\z/.match(obj)
  end

  def eradicate_records_opaque(type)
    objs = []
    pids = File.open("#{type}_to_migrate.txt").read
    pids.each_line do |pid|
      begin
        puts "Get #{pid}"
        work = ActiveFedora::Base.find(pid.squish)
        work.delete
        ActiveFedora::Base.eradicate(pid.squish)
      rescue ActiveFedora::ObjectNotFoundError
        # no-op
      rescue Ldp::Gone
        puts "#{pid} doesn't exist"
      end
    end
  end


  def eradicate_records(type)
    objs = []
    pids = File.open("#{type}_to_migrate.txt").read
    pids.each_line do |pid|
      begin
        pid = convert_id(pid.squish)
        puts "Get #{pid}"
        work = ActiveFedora::Base.find(pid)
        work.delete
        ActiveFedora::Base.eradicate(pid)
      rescue ActiveFedora::ObjectNotFoundError
        # no-op
      rescue Ldp::Gone
        puts "#{pid} doesn't exist"
      end
    end
  end

  desc "Eradicate pdfs"
  task eradicate_new_objs: :environment do
    eradicate_records_opaque('eradicate')
  end

  desc "Eradicate pdfs"
  task eradicate_pdfs: :environment do
    eradicate_records('pdfs')
  end

  desc "Eradicate videos"
  task eradicate_audios: :environment do
    eradicate_records('videos')
  end

  desc "Eradicate audios"
  task eradicate_audios: :environment do
    eradicate_records('audios')
  end

  desc "Eradicate teis"
  task eradicate_teis: :environment do
    eradicate_records('teis')
  end

  desc "Eradicate images"
  task eradicate_images: :environment do
    eradicate_records('images')
  end

  desc "Eradicate eads"
  task eradicate_eads: :environment do
    eradicate_records('eads')
  end

  desc "Eradicate elections"
  task eradicate_elections: :environment do
    eradicate_records('elections')
  end

  desc "Eradicate objects"
  task eradicate_objects: :environment do
    eradicate_records('objects')
  end

  desc "Eradicate generics"
  task eradicate_generics: :environment do
    eradicate_records('generics')
  end
  desc "Migrate election records"
  task migrate_elections: :environment do
    # Specifies FedoraMigrate should use the elections target constructor
    results = FedoraMigrate.migrate_repository(namespace: "tufts", options: {target_constructor: 'elections', repo_type: 'tdr'})
    puts results
  end

  desc "Verify f3 records"
  task verify_f3_records: :environment do
    FedoraMigrate.verify_f3_repository(namespace: "draft", options: {target_constructor: 'objects', repo_type: 'tdr'})
  end
  desc "Migrate objects"
  task migrate_objects: :environment do
    # Specifies FedoraMigrate should use the elections target constructor
    results = FedoraMigrate.migrate_repository(namespace: "tufts", options: {target_constructor: 'objects', repo_type: 'tdr'})
    puts results
  end

  desc "Migrate objects"
  task migrate_objects2: :environment do
    # Specifies FedoraMigrate should use the elections target constructor
    results = FedoraMigrate.migrate_repository(namespace: "tufts", options: {target_constructor: 'objects2', repo_type: 'tdr'})
    puts results
  end


  desc "Migrate objects"
  task migrate_objects3: :environment do
    # Specifies FedoraMigrate should use the elections target constructor
    results = FedoraMigrate.migrate_repository(namespace: "tufts", options: {target_constructor: 'objects3', repo_type: 'tdr'})
    puts results
  end

  desc "Migrate objects"
  task migrate_objects4: :environment do
    # Specifies FedoraMigrate should use the elections target constructor
    results = FedoraMigrate.migrate_repository(namespace: "tufts", options: {target_constructor: 'objects3', repo_type: 'tdr'})
    puts results
  end

  desc "Migrate objects"
  task migrate_objects5: :environment do
    # Specifies FedoraMigrate should use the elections target constructor
    results = FedoraMigrate.migrate_repository(namespace: "tufts", options: {target_constructor: 'objects3', repo_type: 'tdr'})
    puts results
  end

  desc "Migrate objects"
  task migrate_objects6: :environment do
    # Specifies FedoraMigrate should use the elections target constructor
    results = FedoraMigrate.migrate_repository(namespace: "tufts", options: {target_constructor: 'objects3', repo_type: 'tdr'})
    puts results
  end
  
  desc "Migrate objects"
  task migrate_objects7: :environment do
    # Specifies FedoraMigrate should use the elections target constructor
    results = FedoraMigrate.migrate_repository(namespace: "tufts", options: {target_constructor: 'objects3', repo_type: 'tdr'})
    puts results
  end
  desc "Migrate EADs"
  task migrate_eads: :environment do
    # Specifies FedoraMigrate should use the elections target constructor
    results = FedoraMigrate.migrate_repository(namespace: "tufts", options: {target_constructor: 'eads', repo_type: 'tdr'})
    puts results
  end

  desc "Migrate Images"
  task migrate_images: :environment do
    # Specifies FedoraMigrate should use the elections target constructor
    results = FedoraMigrate.migrate_repository(namespace: "tufts", options: {target_constructor: 'images', repo_type: 'tdr'})
    puts results
  end

  desc "Migrate TEIs"
  task migrate_teis: :environment do
    # Specifies FedoraMigrate should use the elections target constructor
    results = FedoraMigrate.migrate_repository(namespace: "tufts", options: {target_constructor: 'teis', repo_type: 'tdr'})
    puts results
  end

  desc "Migrate PDF"
  task migrate_pdfs: :environment do
    # Specifies FedoraMigrate should use the elections target constructor
    results = FedoraMigrate.migrate_repository(namespace: "tufts", options: {target_constructor: 'pdfs', repo_type: 'tdr'})
    puts results
  end

  desc "Migrate Audio"
  task migrate_audio: :environment do
    # Specifies FedoraMigrate should use the elections target constructor
    results = FedoraMigrate.migrate_repository(namespace: "tufts", options: {target_constructor: 'audio', repo_type: 'tdr'})
    puts results
  end

  desc "Migrate Video"
  task migrate_video: :environment do
    # Specifies FedoraMigrate should use the elections target constructor
    results = FedoraMigrate.migrate_repository(namespace: "tufts", options: {target_constructor: 'video', repo_type: 'tdr'})
    puts results
  end

  desc "Migrate RCR"
  task migrate_rcrs: :environment do
    # Specifies FedoraMigrate should use the elections target constructor
    results = FedoraMigrate.migrate_repository(namespace: "tufts", options: {target_constructor: 'rcrs', repo_type: 'tdr'})
    puts results
  end

  desc "Migrate Generics -- only works on campus"
  task migrate_generics: :environment do
    # Specifies FedoraMigrate should use the elections target constructor
    results = FedoraMigrate.migrate_repository(namespace: "tufts", options: {target_constructor: 'generics', repo_type: 'tdr'})
    puts results
  end


  desc "Delete all the content in Fedora 4"
  task clean4: :environment do
    ActiveFedora::Cleaner.clean!
  end
end
