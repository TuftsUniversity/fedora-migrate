desc "Migrate all my objects"
namespace :tufts do

  desc "define top level collections"
  task define_top_level_collections: :environment do
    # top level collections
    collections = ['Manuscripts','New Nation Votes','Test Items','Tufts Scholarship','University Archives','University Publications']
      collections.each do |collection|
        if Collection.where(title: collection).empty?
          a = Collection.new(title: [collection])
          a.apply_depositor_metadata 'mkorcy01'
          a.visibility = Hydra::AccessControls::AccessRight::VISIBILITY_TEXT_VALUE_PUBLIC
          a.save!
        end
      end
  end

  desc "define secondary collections"
  task define_secondary_collections: :environment do
    spec = Gem::Specification.find_by_name("fedora-migrate")
    gem_root = spec.gem_dir
    row_count = 0
    CSV.foreach("#{gem_root}/collection_map.csv", :headers => true, :header_converters => :symbol, :converters => :all) do |row|
      puts row_count
      row_count = row_count + 1
      child_col = row[1]
      if Collection.where(title: child_col).empty?
        next if child_col.blank?
        a = Collection.new(title: [child_col])
        a.apply_depositor_metadata 'mkorcy01'
        a.visibility = Hydra::AccessControls::AccessRight::VISIBILITY_TEXT_VALUE_PUBLIC
        a.save!

        b = Collection.where(title: row[0])
        unless b.empty?
          b.first.add_members a.id
          b.first.save
        end
      end
    end
  end

  desc "delete all collections"
  task delete_all_collections: :environment do
    Collection.all.each {|col| col.delete }
  end

  desc "import collection mapping table"
  task import_collections: :environment do
    spec = Gem::Specification.find_by_name("fedora-migrate")
    gem_root = spec.gem_dir

    db = SQLite3::Database.open "#{gem_root}/collections.sqlite3"

    CSV.foreach("#{gem_root}/pid_map_collections.csv", :headers => true, :header_converters => :symbol, :converters => :all) do |row|

      #CREATE TABLE collection_map(collection, pid);
      pid = row[1]
      col = Collection.where(title: row[0]).first.id
      db.execute "INSERT INTO collection_map VALUES(\"#{col}\",\"#{pid}\")"
    end

    db.close if db

  end

  task migrate: :environment do
    results = FedoraMigrate.migrate_repository(namespace: "tufts", options: {})
    puts results
  end

  desc "Migrate election records"
  task migrate_elections: :environment do
    # Specifies FedoraMigrate should use the elections target constructor
    results = FedoraMigrate.migrate_repository(namespace: "tufts", options: {target_constructor: 'elections'})
    puts results
  end

  desc "Migrate EADs"
  task migrate_eads: :environment do
    # Specifies FedoraMigrate should use the elections target constructor
    results = FedoraMigrate.migrate_repository(namespace: "tufts", options: {target_constructor: 'eads'})
    puts results
  end

  desc "Migrate Images"
  task migrate_images: :environment do
    # Specifies FedoraMigrate should use the elections target constructor
    results = FedoraMigrate.migrate_repository(namespace: "tufts", options: {target_constructor: 'images'})
    puts results
  end

  desc "Migrate TEIs"
  task migrate_teis: :environment do
    # Specifies FedoraMigrate should use the elections target constructor
    results = FedoraMigrate.migrate_repository(namespace: "tufts", options: {target_constructor: 'teis'})
    puts results
  end

  desc "Migrate PDF"
  task migrate_pdfs: :environment do
    # Specifies FedoraMigrate should use the elections target constructor
    results = FedoraMigrate.migrate_repository(namespace: "tufts", options: {target_constructor: 'pdfs'})
    puts results
  end

  desc "Migrate Audio"
  task migrate_audio: :environment do
    # Specifies FedoraMigrate should use the elections target constructor
    results = FedoraMigrate.migrate_repository(namespace: "tufts", options: {target_constructor: 'audio'})
    puts results
  end

  desc "Migrate Video"
  task migrate_video: :environment do
    # Specifies FedoraMigrate should use the elections target constructor
    results = FedoraMigrate.migrate_repository(namespace: "tufts", options: {target_constructor: 'video'})
    puts results
  end

  desc "Migrate RCR"
  task migrate_rcrs: :environment do
    # Specifies FedoraMigrate should use the elections target constructor
    results = FedoraMigrate.migrate_repository(namespace: "tufts", options: {target_constructor: 'rcrs'})
    puts results
  end

  desc "Migrate Generics -- only works on campus"
  task migrate_generics: :environment do
    # Specifies FedoraMigrate should use the elections target constructor
    results = FedoraMigrate.migrate_repository(namespace: "tufts", options: {target_constructor: 'generics'})
    puts results
  end


  desc "Delete all the content in Fedora 4"
  task clean4: :environment do
    ActiveFedora::Cleaner.clean!
  end
end
