require 'byebug'
module FedoraMigrate
  class ObjectMover < Mover
    RIGHTS_DATASTREAM = "rightsMetadata".freeze

    ContentDatastreamReport = Struct.new(:ds, :versions)
    RDFDatastreamReport = Struct.new(:ds, :status)
    Report = Struct.new(:id, :fedora_3_pid, :class, :content_datastreams, :rdf_datastreams, :permissions, :dates)

    def migrate
      prepare_target
      #conversions.collect { |ds| convert_rdf_datastream(ds) }
      migrate_datastreams
      complete_target
      super
    end

    def post_initialize
      conversion_options
      create_target_model if target.nil?
    end

    def results_report
      Report.new.tap do |report|
        report.content_datastreams = []
        report.rdf_datastreams = []
      end
    end

    def prepare_target
      report.class = target.class.to_s
      report.id = target.id
      before_object_migration
    end

    def complete_target
      after_object_migration
      report.id = target.id
      report.fedora_3_pid = source.pid

      save
    end

    def target
      #TDLR-728 for details

      #%w(tufts:UA197.002.002.00002 tufts:18776 tufts:18774 tufts:18933 tufts:MS046.006.00003 tufts:MS046.006.00002 tufts:MS046.006.00001)

      xml = Nokogiri::XML(source.datastreams['DCA-ADMIN'].content).remove_namespaces!
      field_values = xml.xpath("//steward")
      target_value = 'tisch'
      unless field_values.empty?
        target_value = field_values.first.text
      else
        target_value = ''
      end
      utln = 'migration'

      if target_value == 'tisch'
        utln='amay02'
      elsif target_value == 'sample'
        utln='mkorcy01'
      elsif target_value == 'library'
        utln='amay02'
      end

      if @options[:target_constructor] == 'elections'
        @target ||= FedoraMigrate::TuftsTargetConstructor.new(source,'RECORD-XML', nil, 'ebeck01', @keep_extent).build
      elsif @options[:target_constructor] == 'eads'
        @target ||= FedoraMigrate::TuftsTargetConstructor.new(source, 'Archival.xml', nil, utln, @keep_extent).build
      elsif @options[:target_constructor] == 'images'
        @target ||= FedoraMigrate::TuftsTargetConstructor.new(source, 'Archival.tif', nil, utln, @keep_extent).build
      elsif @options[:target_constructor] == 'pdfs'
        @target ||= FedoraMigrate::TuftsTargetConstructor.new(source, 'Archival.pdf', 'Transfer.binary', utln, @keep_extent).build
      elsif @options[:target_constructor] == 'teis'
        @target ||= FedoraMigrate::TuftsTargetConstructor.new(source, 'Archival.xml', nil, utln, @keep_extent).build
      elsif @options[:target_constructor] == 'audio'
        @target ||= FedoraMigrate::TuftsTargetConstructor.new(source, 'ARCHIVAL_WAV', 'ARCHIVAL_XML', utln, @keep_extent).build
      elsif @options[:target_constructor] == 'video'
        @target ||= FedoraMigrate::TuftsTargetConstructor.new(source, 'Archival.video', 'ARCHIVAL_XML', utln, @keep_extent).build
      elsif @options[:target_constructor] == 'rcrs'
        @target ||= FedoraMigrate::TuftsTargetConstructor.new(source, 'RCR-CONTENT', nil, utln, @keep_extent).build
      elsif @options[:target_constructor] == 'generics'
        @target ||= FedoraMigrate::TuftsTargetConstructor.new(source, 'GENERIC-CONTENT', nil, utln, @keep_extent).build
      else
        if (source.profile['objModels'] & ["info:fedora/afmodel:TuftsVotingRecord", "info:fedora/cm:VotingRecord"]).any?
          @target ||= FedoraMigrate::TuftsTargetConstructor.new(source,'RECORD-XML', nil, 'ebeck01', @keep_extent).build
        elsif (source.profile['objModels'] & ["info:fedora/cm:Text.EAD", "info:fedora/afmodel:TuftsEAD"]).any?
          @target ||= FedoraMigrate::TuftsTargetConstructor.new(source, 'Archival.xml', nil, utln, @keep_extent).build
        elsif (source.profile['objModels'] & ["info:fedora/cm:Image.4DS", "info:fedora/cm:Image.3DS", "info:fedora/afmodel:TuftsImage"]).any?
          @target ||= FedoraMigrate::TuftsTargetConstructor.new(source, 'Archival.tif', nil, utln, @keep_extent).build
        elsif (source.profile['objModels'] & ["info:fedora/cm:Text.PDF", "info:fedora/afmodel:TuftsPdf", "info:fedora/cm:Text.FacPub", "info:fedora/afmodel:TuftsFacultyPublication"]).any?
          @target ||= FedoraMigrate::TuftsTargetConstructor.new(source, 'Archival.pdf', 'Transfer.binary', utln, @keep_extent).build
        elsif (source.profile['objModels'] & ["info:fedora/afmodel:TuftsTEI", "info:fedora/cm:Text.TEI"]).any?
          @target ||= FedoraMigrate::TuftsTargetConstructor.new(source, 'Archival.xml', nil, utln, @keep_extent).build
        elsif (source.profile['objModels'] & ["info:fedora/cm:Audio", "info:fedora/afmodel:TuftsAudio", "info:fedora/cm:Audio.OralHistory", "info:fedora/afmodel:TuftsAudioText"]).any?
          @target ||= FedoraMigrate::TuftsTargetConstructor.new(source, 'ARCHIVAL_WAV', 'ARCHIVAL_XML', utln, @keep_extent).build
        elsif (source.profile['objModels'] & ["info:fedora/afmodel:TuftsVideo"]).any?
          @target ||= FedoraMigrate::TuftsTargetConstructor.new(source, 'Archival.video', 'ARCHIVAL_XML', utln, @keep_extent).build
        elsif (source.profile['objModels'] & ["info:fedora/cm:Text.RCR", "info:fedora/afmodel:TuftsRCR"]).any?
          @target ||= FedoraMigrate::TuftsTargetConstructor.new(source, 'RCR-CONTENT', nil, utln, @keep_extent).build
        elsif (source.profile['objModels'] & ["info:fedora/cm:Object.Generic", "info:fedora/afmodel:TuftsGenericObject"]).any?
          @target ||= FedoraMigrate::TuftsTargetConstructor.new(source, 'GENERIC-CONTENT', nil, utln, @keep_extent).build
        else
          puts "** UNKNOWN Target **"
          exit
        end
      end

      @target
    end

    private

    def migrate_datastreams
      migrate_content_datastreams
  #    migrate_permissions
#      migrate_dates
    end

    # We have to call save before migrating content datastreams, otherwise versions aren't recorded
    # TODO: this will fail if required fields are defined in a descMetadata datastream that is not
    # converted to RDF (issue #8)
    def migrate_content_datastreams
      save
      target.attached_files.keys.each do |ds|
        mover = FedoraMigrate::DatastreamMover.new(source.datastreams[ds.to_s], target.attached_files[ds.to_s], options)
        report.content_datastreams << ContentDatastreamReport.new(ds, mover.migrate)
      end
    end

    def convert_rdf_datastream(ds)
      return unless source.datastreams.key?(ds)
      mover = FedoraMigrate::RDFDatastreamMover.new(datastream_content(ds), target)
      report.rdf_datastreams << RDFDatastreamReport.new(ds, mover.migrate)
    end

    def datastream_content(dsid)
      source.datastreams[dsid.to_s]
    end

    def migrate_permissions
      return unless source.datastreams.keys.include?(RIGHTS_DATASTREAM) && target.respond_to?(:permissions)
      mover = FedoraMigrate::PermissionsMover.new(source.datastreams[RIGHTS_DATASTREAM], target)
      report.permissions = mover.migrate
    end

    def migrate_dates
      report.dates = FedoraMigrate::DatesMover.new(source, target).migrate
    end
  end
end
