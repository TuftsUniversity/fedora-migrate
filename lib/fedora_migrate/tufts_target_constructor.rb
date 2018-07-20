require 'open-uri'
require 'uri'
require 'hyrax'
require 'byebug'
#require 'rdf/fcrepo3'

module FedoraMigrate
  class TuftsTargetConstructor
    attr_accessor :source, :candidates, :target, :keep_extent
    attr_accessor :payload_primary, :payload_secondary, :depositor_utln

    def initialize(source, payload_stream, secondary_payload_stream, depositor, keep_extent)
      @source = source
      @payload_primary = payload_stream
      @payload_secondary = secondary_payload_stream
      @depositor_utln = depositor
      @keep_extent = keep_extent
      #@logger = Logger.new("#{Rails.root}/log/migration.log")
    end

    # Convert a legacy Tufts identifier into a predictable and valid Fedora identifier
    # @param [String] id
    # @return [String]
    def convert_id(id)
      newid = id.downcase
      newid.slice!('tufts:')
      newid.tr!('.', '_')

      if number?(newid)
        newid =  "pid" + newid
      end

      newid
    end

    # Override .build to use Fedora's default id minter
    # https://github.com/projecthydra-labs/fedora-migrate/issues/49
    #obj.id FedoraMigrate::Mover.id_component(source)
    def build
      raise FedoraMigrate::Errors::MigrationError, "No qualified targets found in #{source.pid}" if target.nil?
      admin_set = AdminSet.find(AdminSet::DEFAULT_ID)
      # use predictable ids till we get this working
      if true
        obj = target.new(id: convert_id(source.pid))
      else
        obj = target.new
      end

      # set core data
      obj.admin_set = admin_set
      obj.apply_depositor_metadata @depositor_utln
      if source.pid.include? "perseus"
        obj.visibility = Hydra::AccessControls::AccessRight::VISIBILITY_TEXT_VALUE_PRIVATE
      else 
        obj.visibility = Hydra::AccessControls::AccessRight::VISIBILITY_TEXT_VALUE_PUBLIC
      end
      obj.date_uploaded =  DateTime.parse(source.profile['objCreateDate'])
      obj.date_modified = DateTime.current.to_date

      build_filesets obj
      process_metadata obj

      obj.reload

#      obj.file_sets.each do |file_set|
#        CreateDerivativesJob.perform_now(file_set, file_set.public_send(:original_file).id) unless file_set.public_send(:original_file).nil?
#      end

      put_object_into_workflow obj

      obj
    end

    def process_metadata obj
      process_desc_metadata obj
      process_admin_metadata obj
      # process_relsext_metadata obj
      obj.save

      process_collection_metadata obj
      obj.reload
      obj.save
    end

    def build_filesets obj
      create_and_add_payload(obj, @payload_primary, @depositor_utln)

      #deal with 2 primary datastream objects, storing second object in a new file set
      create_and_add_payload(obj, @payload_secondary, @depositor_utln) unless @payload_secondary.nil?

      #handle a case of bad hand created data on old records
      create_and_add_payload(obj, "ARCHIVAL_SOUND", @depositor_utln) if @payload_primary == "ARCHIVAL_WAV"
    end

    def put_object_into_workflow object
      user = ::User.find_by(username: @depositor_utln)
      subject = Hyrax::WorkflowActionInfo.new(object, user)

      begin
        Hyrax::Workflow::WorkflowFactory.create(object, {}, user)
      rescue ActiveRecord::RecordNotUnique
        puts "Already in Workflow"
      end
      tries = 20
      begin
        object = object.reload
        sipity_workflow_action = PowerConverter.convert_to_sipity_action("publish", scope: subject.entity.workflow) { nil }
        Hyrax::Workflow::WorkflowActionService.run(subject: subject, action: sipity_workflow_action , comment: "Migrated from Fedora 3")
      rescue NoMethodError
        tries -= 1
        if tries > 0
          sleep(5.seconds)
          retry
        else
          Rails.logger.error "Fixture file missing original for #{@source.pid}"
        end
      end
    end

    def create_and_add_payload(obj, payload_stream, depositor_utln)
#/byebug
      # create fileset and apply depository metadata
      #request = Net::HTTP::Get.new uri.request_uri
      #uri = URI(obj.datastreams[payload_stream])
      #http.request request do |response|
      #  File.open '/tmp/blah', 'w' do |io|
      #    response.read_body do |chunk|
      #    io.write chunk
      #  end
      #@end

      return if source.datastreams[payload_stream].size.nil?
      file_set = FileSet.new
      file_set.apply_depositor_metadata depositor_utln

      if payload_stream == "GENERIC-CONTENT"
        @doc = Nokogiri::XML(source.datastreams[payload_stream].content).remove_namespaces!
        location = @doc.xpath('//link').text
        uri = URI.parse(location)
        target_file = File.basename(uri.path)
        file_set.title = [target_file]
        file_set.label = target_file
      else
        file_set.title = [source.datastreams[payload_stream].label]
        file_set.label = source.datastreams[payload_stream].label
      end

      if source.pid.include? "perseus"
        file_set.visibility = Hydra::AccessControls::AccessRight::VISIBILITY_TEXT_VALUE_PRIVATE
      else 
        file_set.visibility = Hydra::AccessControls::AccessRight::VISIBILITY_TEXT_VALUE_PUBLIC
      end
      work_permissions = obj.permissions.map(&:to_hash)

      if payload_stream == "GENERIC-CONTENT"
        datastream_content = get_generic_file_from_source(payload_stream)
      else

        datastream_content = get_file_from_source(payload_stream)
      end

      user = ::User.find_by(username: file_set.depositor)
      actor = Hyrax::Actors::FileSetActor.new(file_set, user)
      if source.pid.include? "perseus"
        actor.create_metadata("visibility" => Hydra::AccessControls::AccessRight::VISIBILITY_TEXT_VALUE_PRIVATE)
      else 
        actor.create_metadata("visibility" => Hydra::AccessControls::AccessRight::VISIBILITY_TEXT_VALUE_PRIVATE)
      end

      actor.create_content(datastream_content)
      actor.attach_to_work(obj)
      actor.file_set.permissions_attributes = work_permissions
      file_set.save
    end

    def number?(obj)
      obj = obj.to_s unless obj.is_a? String
      /\A[+-]?\d+(\.[\d]+)?\z/.match(obj)
    end

    def process_admin_metadata obj
#      field = process_metadata_field('date_submitted', 'DC-DETAIL-META', false)
#      obj.date_uploaded = field unless field == ''

      field = process_metadata_field('date.issued', 'DCA-META')
      obj.date_issued = field unless field.empty?

      # TODO: Review from spreadsheet
      field = process_metadata_field('date.available', 'DCA-META')
      obj.date_available = field unless field == ''

      # Use DC Datastream modified
#      field = process_metadata_field('date_modified', 'DC-DETAIL-META', false)
#      obj.date_modified = field unless field == ''


      field = process_metadata_field('license', 'DC-DETAIL-META')
      obj.license = field unless field == ''

      field = process_metadata_field('accrualPolicy', 'DC-DETAIL-META', false)
      obj.accrual_policy = field unless field == ''

      field = process_metadata_field('steward', 'DCA-ADMIN', false)
      obj.steward = field unless field == ''

      field = process_metadata_field('download', 'DCA-ADMIN', false)
      obj.downloadable = field unless field == ''

      field = process_metadata_field('comment', 'DCA-ADMIN')
      obj.internal_note =field unless field.empty?

      field = process_metadata_field('displays', 'DCA-ADMIN')
      obj.displays_in = field unless field.empty?

      field = process_metadata_field('qrStatus', 'DCA-ADMIN')
      obj.qr_status = field unless field.empty?

      field = process_metadata_field('note', 'DCA-ADMIN')
      obj.qr_note = field unless field.empty?

      field = process_metadata_field('rejectionReason', 'DCA-ADMIN')
      obj.rejection_reason = field unless field.empty?

      field = process_metadata_field('createdby', 'DCA-ADMIN')
      unless field.empty?
        if field.include? 'externalXSLT'
          steward = process_metadata_field('steward', 'DCA-ADMIN', false)
          field = ["amay02 via batch ingest"] if steward == 'tisch'
        end
      end
      obj.createdby = field unless field.empty?
      #embargo

      field = process_metadata_field('startDate', 'DCA-ADMIN')
      obj.admin_start_date = field unless field.empty?

      field = process_metadata_field('retentionPeriod', 'DCA-ADMIN')
      obj.retention_period = field unless field.empty?

      field = process_metadata_field('embargo', 'DCA-ADMIN', false)
      obj.embargo_note = field unless field == ''

      field = process_metadata_field('expDate', 'DCA-ADMIN', false)
      obj.end_date = field unless field == ''

#      field = process_metadata_field('batchID', 'DCA-ADMIN', false)
#      obj.batch_id = field unless field == ''

      field = process_metadata_field('audience', 'DC-DETAIL-META', false)
      obj.audience = field unless field == ''

      field = process_metadata_field('dateAccepted', 'DC-DETAIL-META')
      obj.date_accepted = field unless field.empty?

      field = process_metadata_field('dateCopyrighted', 'DC-DETAIL-META')
      obj.date_copyrighted = field unless field.empty?

      field = process_metadata_field('creatordept', 'DCA-ADMIN', true)
      obj.creator_department = field unless field == ''
    end

    def process_collection_metadata obj
      # add to collection

      spec = Gem::Specification.find_by_name("fedora-migrate")
      gem_root = spec.gem_dir

      db = SQLite3::Database.open "#{gem_root}/collections.sqlite3"
      row = db.get_first_row "SELECT collection FROM collection_map WHERE pid=\"#{source.pid}\""
      unless row.nil?
        col_id =  row.join.strip
        col = Collection.find(col_id)
        obj.member_of_collections = [col]
        obj.save!
      end

    end

    def process_relsext_metadata obj

      xml = Nokogiri::XML(source.datastreams["RELS-EXT"].content).remove_namespaces!

      RDF::FCREPO3::RELSEXT.properties.each do |property|

        val = property.value
        short_code = val[val.index('#')+1..val.length]
        field_values = xml.xpath("//#{short_code}")
        target_values = Array.new

        field_values.each do |field|
          (target_values << field.values.first) unless field.nil?
        end

        obj.send("relsext_#{short_code}=", target_values) unless target_values.length == 0

      end
    end

    def process_desc_metadata obj
      val = source.pid
      obj.legacy_pid = val unless val.nil?

      field = process_metadata_field('date.created', 'DCA-META')
      obj.date_created = field unless field == ''

      val = process_metadata_field('title', 'DCA-META')
      obj.title = val unless val.empty?

      val = process_metadata_field('alternative', 'DC-DETAIL-META')
      obj.alternative_title  = val unless val.empty?

      val = process_metadata_field('creator', 'DCA-META')
      obj.creator = val unless val.empty?

      val = process_metadata_field('isPartOf', 'DCA-META')
      obj.is_part_of = val unless val.empty?

      val = process_metadata_field('contributor', 'DC-DETAIL-META')
      obj.contributor = val unless val.empty?

      val = process_metadata_field('description', 'DCA-META')
      obj.description = val unless val.empty?

      val = process_metadata_field('abstract', 'DC-DETAIL-META')
      obj.abstract = val unless val.empty?

      val = process_metadata_field('publisher', 'DCA-META')
      obj.publisher = val unless val.empty?

      val = process_metadata_field('source', 'DCA-META')
      obj.source = val unless val.empty?

      val = process_metadata_field('date', 'DCA-META')
      obj.primary_date = val unless val.empty?

      val = process_metadata_field('language', 'DC-DETAIL-META')
      obj.language = val unless val.empty?

      val = process_metadata_field('persname', 'DCA-META')
      obj.personal_name = val unless val.empty?

      val = process_metadata_field('corpname', 'DCA-META')
      obj.corporate_name = val unless val.empty?

      val = process_metadata_field('geogname', 'DCA-META')
      obj.geog_name = val unless val.empty?

      val = process_metadata_field('subject', 'DCA-META')
      obj.subject = val unless val.empty?

      val = process_metadata_field('genre', 'DC-DETAIL-META')
      obj.genre = val unless val.empty?

      val = process_metadata_field('spatial', 'DC-DETAIL-META')
      obj.geographic_name = val unless val.empty?

      val = process_metadata_field('bibliographicCitation', 'DCA-META')
      obj.bibliographic_citation = val unless val.empty?

      val = process_metadata_field('temporal', 'DCA-META')
      obj.temporal = val unless val.empty?

      val = process_metadata_field('identifier', 'DCA-META')
      obj.identifier = val unless val.empty?

#      val = process_metadata_field('references', 'DC-DETAIL-META')
#      obj.references = val unless val.empty?

      val = process_metadata_field('replaces', 'DC-DETAIL-META')
      obj.replaces = val unless val.empty?

      val = process_metadata_field('toc', 'DC-DETAIL-META')
      obj.table_of_contents = val unless val.empty?

      val = process_metadata_field('isReplacedBy', 'DC-DETAIL-META')
      obj.is_replaced_by = val unless val.empty?

      val = process_metadata_field('hasPart', 'DC-DETAIL-META')
      obj.has_part = val unless val.empty?

      val = process_metadata_field('provenance', 'DC-DETAIL-META')
      obj.provenance = val unless val.empty?

      val = process_metadata_field('rightsHolder', 'DC-DETAIL-META')
      obj.rights_holder = val unless val.empty?

      field = process_metadata_field('accessRights', 'DC-DETAIL-META', false)
      obj.rights_note = field unless field == ''

      val = process_metadata_field('funder', 'DCA-META')
      additional_funders = process_metadata_field('funder', 'DC-DETAIL-META')
      val = val + additional_funders
      obj.funder = val unless val.empty?

      val = process_metadata_field('format', 'DCA-META')
      obj.format_label = val unless val.empty?

      val = process_metadata_field('rights', 'DCA-META')
      unless val.empty?
        known = ["http://dca.tufts.edu/ua/access/rights-pd.html",
                 "http://dca.tufts.edu/ua/access/rights.html",
                 "http://sites.tufts.edu/dca/research-help/copyright-and-citations/",
                 "http://sites.tufts.edu/dca/about-us/research-help/reproductions-and-use/"]
        unless (known & val).empty?
          val = ["http://sites.tufts.edu/dca/about-us/research-help/reproductions-and-use/"]
        end
        if source.pid.include? 'perseus'
          val = ["http://rightsstatements.org/page/CNE/1.0/?language=en"]
        end
        obj.rights_statement = val
      end

      val  = process_metadata_field('type', 'DCA-META')
      # temporary fix for perseus but we should work this out for other types
      if val.sort == ['image']
        val = ['Image']
      end
      obj.resource_type = val unless val.empty?

      # do not keep extents in the list
      unless @keep_extent.include?(source.pid)
        val = process_metadata_field('extent', 'DCA-META', true)
        obj.extent = val unless val.empty?
      end

    end

    def process_metadata_field(field_name,datastream_name,multiple=true)
      xml = Nokogiri::XML(source.datastreams[datastream_name].content).remove_namespaces!
      field_values = xml.xpath("//#{field_name}")
      if multiple
        target_values = Array.new
        field_values.each do |field|
          (target_values << field.text) unless field.nil?
        end
      else
        unless field_values.empty?
          target_values = field_values.first.text
        else
          target_values = ''
        end
      end

      target_values
    end

    def target
      @target ||= determine_target
    end

    private

    def get_file_from_source(datastream)
      uri = URI.parse(source.datastreams[datastream].location)
      target_file = File.basename(uri.path)
      http = Net::HTTP.new(uri.host, uri.port)

      voting_record = File.new target_file, 'wb'
      http.request_get(uri.path) do |response|
        case response
        when Net::HTTPNotFound
          output "404 - Not Found"
          return false
        when Net::HTTPOK

          response.read_body do |chunk|
            voting_record << chunk
            voting_record.flush
          end
          voting_record.close
        end
      end
      File.new target_file
    end

    def get_generic_file_from_source(datastream)
      @doc = Nokogiri::XML(source.datastreams[datastream].content).remove_namespaces!
      location = @doc.xpath('//link').text

      uri = URI.parse(location)
      target_file = File.basename(uri.path)

      voting_record = File.new target_file, 'wb'
      voting_record.write Net::HTTP.get(uri)
      voting_record.flush
      voting_record.close

      File.new target_file
    end

    def determine_target

      Array(candidates).map { |model| vet(model) }.compact.first
    end

    def vet(model)
      FedoraMigrate::Mover.id_component(model).constantize
    rescue NameError
      Logger.debug "rejecting #{model} for target"
      nil
    end

    def candidates
      @candidates = Array(source.models).map do |candidate|
        if candidate == "info:fedora/cm:Text.PDF"
          "Pdf"
        elsif candidate == "info:fedora/cm:Text.TEI"
          "Tei"
        elsif candidate == "info:fedora/cm:VotingRecord"
          "VotingRecord"
        elsif candidate == "info:fedora/cm:Text.EAD"
          "Ead"
        elsif candidate == "info:fedora/cm:Image.4DS"
          "Image"
        elsif candidate == "info:fedora/cm:Audio"
          "Audio"
        elsif candidate == "info:fedora/cm:Audio.OralHistory"
          "Audio"
        elsif candidate == "info:fedora/afmodel:TuftsVideo"
          "Video"
        elsif candidate == "info:fedora/cm:Text.RCR"
          "Rcr"
        elsif candidate == "info:fedora/cm:Object.Generic"
          "GenericObject"
        else
          candidate
        end
      end
    end
  end


end
